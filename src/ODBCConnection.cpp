/* -*- indent-tabs-mode: nil -*- */
/*
    ODBCConnection.cpp

    Qore ODBC module

    Copyright (C) 2016 - 2022 Qore Technologies s.r.o.

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
*/

#include "ODBCConnection.h"

#include "qore/QoreLib.h"
#include "qore/DBI.h"

#include "ODBCErrorHelper.h"
#include "ODBCStatement.h"

namespace odbc {

ODBCConnection::ODBCConnection(Datasource* d, ExceptionSink* xsink) : ds(d), connStr(QCS_UTF8) {
    // Parse options passed through the datasource.
    if (parseOptions(xsink)) {
        return;
    }

    // Timezone handling.
    if (!serverTz) {
        DateTime d(false); // Used just to get the local zone info.
        serverTz = d.getZone();
    }

    // set Qore encoding directly from DB encoding
    {
        const char* enc = d->getDBEncoding();
        d->setQoreEncoding(enc ? enc : "UTF-8");
    }

    // Initialize environment.
    if (envInit(xsink)) {
        return;
    }

    // Prepare ODBC connection string.
    if (prepareConnectionString(xsink)) {
        return;
    }

    // Connect.
    if (connect(xsink)) {
        return;
    }

    // Get DBMS (server) and ODBC DB driver (client) versions.
    getVersions();
}

ODBCConnection::~ODBCConnection() {
    disconnect();

    // Free up allocated handles.
    if (dbc != SQL_NULL_HDBC) {
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env != SQL_NULL_HENV) {
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }
}

int ODBCConnection::connect(ExceptionSink* xsink) {
    SQLRETURN ret;

    // Free up allocated handle.
    if (dbc != SQL_NULL_HDBC) {
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        dbc = SQL_NULL_HDBC;
    }

    // Allocate a connection handle.
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("ODBC-CONNECTION-HANDLE-ERROR", "could not allocate a connection handle");
        return -1;
    }

    // Set connection attributes.
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbc, SQL_ATTR_QUIET_MODE, 0, SQL_IS_POINTER);
    SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)(size_t)options.loginTimeout, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)(size_t)options.connTimeout, SQL_IS_UINTEGER);

    // Connect
    ret = SQLDriverConnectA(dbc, 0, (SQLCHAR*)connStr.c_str(), connStr.length(), 0, 0, 0, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("could not connect to the datasource; connection string: '%s'");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_DBC, dbc, s);
        xsink->raiseException("ODBC-CONNECTION-ERROR", s.c_str(), connStr.c_str());
        return -1;
    }
    connected = true;

    return 0;
}

void ODBCConnection::disconnect() {
    while (connected) {
        SQLRETURN ret = SQLDisconnect(dbc);
        if (SQL_SUCCEEDED(ret)) {
            connected = false;
        } else {
            char state[7];
            memset(state, 0, sizeof(state));
            ODBCErrorHelper::extractState(SQL_HANDLE_DBC, dbc, state);
            //fprintf(stderr, "state: %s\n", state);
            if (strncmp(state, "08003", 5) == 0) { // Connection not open.
                connected = false;
            } else if (strncmp(state, "HY000", 5) == 0) { // General error.
                connected = false;
            } else if (strncmp(state, "HYT01", 5) == 0) { // Connection timeout expired.
                connected = false;
            } else if (strncmp(state, "HY117", 5) == 0) { // Connection is suspended due to unknown transaction state. Only disconnect and read-only functions are allowed.
                connected = false;
            } else if (strncmp(state, "25000", 5) == 0) { // Transaction still active.
                if (!activeTransaction) {
                    if (isDead)
                        isDead = false;
                    rollback(0);
                }
            } else {
                qore_usleep(50*1000); // Sleep in intervals of 50 ms until disconnected.
            }
        }
    }
}

int ODBCConnection::commit(ExceptionSink* xsink) {
    if (isDead) {
        deadConnectionError(xsink);
        return -1;
    }

    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("ODBC-COMMIT-ERROR", "could not commit the transaction", xsink);
        return -1;
    }
    activeTransaction = false;
    return 0;
}

int ODBCConnection::rollback(ExceptionSink* xsink) {
    if (isDead) {
        deadConnectionError(xsink);
        return -1;
    }

    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("ODBC-ROLLBACK-ERROR", "could not rollback the transaction", xsink);
        return -1;
    }
    activeTransaction = false;
    return 0;
}

QoreValue ODBCConnection::select(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return QoreValue();

    if (res.exec(qstr, args, xsink))
        return QoreValue();

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return res.rowsAffected();
}

QoreListNode* ODBCConnection::selectRows(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return nullptr;

    if (res.exec(qstr, args, xsink))
        return nullptr;

    return res.getOutputList(xsink);
}

QoreHashNode* ODBCConnection::selectRow(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return nullptr;

    if (res.exec(qstr, args, xsink))
        return nullptr;

    return res.getSingleRow(xsink);
}

QoreValue ODBCConnection::exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    //fprintf(stderr, "ODBCConnection::exec called: '%s'\n", qstr->c_str());
    ODBCStatement res(this, xsink);
    if (*xsink) {
        return QoreValue();
    }

    if (res.exec(qstr, args, xsink)) {
        //fprintf(stderr, "exec failed\n");
        return QoreValue();
    }
    activeTransaction = true;

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return res.rowsAffected();
}

QoreValue ODBCConnection::execRaw(const QoreString* qstr, ExceptionSink* xsink) {
    //fprintf(stderr, "ODBCConnection::execRaw called: '%s'\n", qstr->c_str());
    ODBCStatement res(this, xsink);
    if (*xsink)
        return QoreValue();

    if (res.exec(qstr, xsink)) {
        //fprintf(stderr, "execRaw failed\n");
        return QoreValue();
    }
    activeTransaction = true;

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return res.rowsAffected();
}

int ODBCConnection::setOption(const char* opt, QoreValue val, ExceptionSink* xsink) {
    if (!strcasecmp(opt, DBI_OPT_NUMBER_OPT)) {
        options.numeric = ENO_OPTIMAL;
        return 0;
    } else if (!strcasecmp(opt, DBI_OPT_NUMBER_STRING)) {
        options.numeric = ENO_STRING;
        return 0;
    } else if (!strcasecmp(opt, DBI_OPT_NUMBER_NUMERIC)) {
        options.numeric = ENO_NUMERIC;
        return 0;
    } else if (!strcasecmp(opt, OPT_BIGINT_NATIVE)) {
        options.bigint = EBO_NATIVE;
        return 0;
    } else if (!strcasecmp(opt, OPT_BIGINT_STRING)) {
        options.bigint = EBO_STRING;
        return 0;
    } else if (!strcasecmp(opt, OPT_FRAC_PRECISION)) {
        return setFracPrecisionOption(val, xsink);
    } else if (!strcasecmp(opt, OPT_QORE_TIMEZONE)) {
        if (val.getType() == NT_STRING) {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            TempEncodingHelper tzName(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            serverTz = find_create_timezone(tzName->c_str(), xsink);
            if (*xsink)
                return -1;
        } else {
            xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires a name of a timezone (e.g. "
                "\"Europe/Prague\") or a time offset string (e.g. \"+02:00\")", OPT_QORE_TIMEZONE);
            return -1;
        }
    } else if (!strcasecmp(opt, OPT_LOGIN_TIMEOUT)) {
        return setLoginTimeoutOption(val, xsink);
    } else if (!strcasecmp(opt, OPT_CONN_TIMEOUT)) {
        return setConnectionTimeoutOption(val, xsink);
    } else if (!strcasecmp(opt, OPT_PRESERVE_CASE)) {
        options.preserve_case = val.getAsBool();
    }

    return 0;
}

QoreValue ODBCConnection::getOption(const char* opt) {
    assert(options.numeric == ENO_OPTIMAL || options.numeric == ENO_STRING || options.numeric == ENO_NUMERIC);
    assert(options.bigint == EBO_NATIVE || options.bigint == EBO_STRING);

    if (!strcasecmp(opt, DBI_OPT_NUMBER_OPT))
        return options.numeric == ENO_OPTIMAL;

    if (!strcasecmp(opt, DBI_OPT_NUMBER_STRING))
        return options.numeric == ENO_STRING;

    if (!strcasecmp(opt, DBI_OPT_NUMBER_NUMERIC))
        return options.numeric == ENO_NUMERIC;

    if (!strcasecmp(opt, OPT_BIGINT_NATIVE))
        return options.bigint == EBO_NATIVE;

    if (!strcasecmp(opt, OPT_BIGINT_STRING))
        return options.bigint == EBO_STRING;

    if (!strcasecmp(opt, OPT_FRAC_PRECISION))
        return static_cast<int64>(options.frPrec);

    if (!strcasecmp(opt, OPT_QORE_TIMEZONE)) {
        if (serverTz) {
            DateTime x(static_cast<int64>(360000));
            x.setZone(serverTz);
            qore_tm info;
            x.getInfo(info);
            return new QoreStringNode(info.regionName());
        } else {
            return new QoreStringNode("UTC");
        }
    }

    if (!strcasecmp(opt, OPT_LOGIN_TIMEOUT))
        return static_cast<int64>(options.connTimeout);

    if (!strcasecmp(opt, OPT_CONN_TIMEOUT))
        return static_cast<int64>(options.loginTimeout);

    if (!strcasecmp(opt, OPT_CONN)) {
        return new QoreStringNode(options.conn);
    }

    return QoreValue();
}

int ODBCConnection::allocStatementHandle(SQLHSTMT& stmt, ExceptionSink* xsink) {
    checkIfConnectionDead(xsink);
    if (*xsink)
        return -1;

    if (isDead) {
        deadConnectionError(xsink);
        return -1;
    }

    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("ODBC-STATEMENT-ALLOC-ERROR", "could not allocate a statement handle", xsink);
        return -1;
    }

    return 0;
}

int ODBCConnection::envInit(ExceptionSink* xsink) {
    SQLRETURN ret;
    // Allocate an environment handle.
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("ODBC-ENV-HANDLE-ERROR", "could not allocate an environment handle");
        return -1;
    }

    // Use ODBC version 3.
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*) SQL_OV_ODBC3, 0);

    return 0;
}

int ODBCConnection::parseOptions(ExceptionSink* xsink) {
    ConstHashIterator hi(ds->getConnectOptions());
    while (hi.next()) {
        if (!strcasecmp(DBI_OPT_NUMBER_OPT, hi.getKey())) {
            options.numeric = ENO_OPTIMAL;
            continue;
        }
        if (!strcasecmp(DBI_OPT_NUMBER_STRING, hi.getKey())) {
            options.numeric = ENO_STRING;
            continue;
        }
        if (!strcasecmp(DBI_OPT_NUMBER_NUMERIC, hi.getKey())) {
            options.numeric = ENO_NUMERIC;
            continue;
        }
        if (!strcasecmp(OPT_BIGINT_NATIVE, hi.getKey())) {
            options.bigint = EBO_NATIVE;
            continue;
        }
        if (!strcasecmp(OPT_BIGINT_STRING, hi.getKey())) {
            options.bigint = EBO_STRING;
            continue;
        }
        if (!strcasecmp(OPT_FRAC_PRECISION, hi.getKey())) {
            QoreValue val = hi.get();
            if (setFracPrecisionOption(val, xsink))
                return -1;
            continue;
        }
        if (!strcasecmp(OPT_QORE_TIMEZONE, hi.getKey())) {
            QoreValue val = hi.get();
            if (val.getType() != NT_STRING) {
                xsink->raiseException("ODBC-OPTION-ERROR", "non-string value passed for the '%s' option",
                    OPT_QORE_TIMEZONE);
                return -1;
            }
            const QoreStringNode* str = val.get<const QoreStringNode>();
            const AbstractQoreZoneInfo* tz = find_create_timezone(str->c_str(), xsink);
            if (*xsink)
                return -1;
            serverTz = tz;
            continue;
        }
        if (!strcasecmp(OPT_LOGIN_TIMEOUT, hi.getKey())) {
            QoreValue val = hi.get();
            if (setLoginTimeoutOption(val, xsink))
                return -1;
            continue;
        }
        if (!strcasecmp(OPT_CONN_TIMEOUT, hi.getKey())) {
            QoreValue val = hi.get();
            if (setConnectionTimeoutOption(val, xsink))
                return -1;
            continue;
        }
        if (!strcasecmp(OPT_CONN, hi.getKey())) {
            QoreValue val = hi.get();
            if (val.getType() != NT_STRING) {
                xsink->raiseException("ODBC-OPTION-ERROR", "non-string value passed for the '%s' option",
                    OPT_QORE_TIMEZONE);
                return -1;
            }
            const QoreStringNode* str = val.get<const QoreStringNode>();
            options.conn = str->c_str();
            continue;
        }
    }
    return 0;
}

int ODBCConnection::setFracPrecisionOption(QoreValue val, ExceptionSink* xsink) {
    if (val) {
        if (val.getType() == NT_INT) {
            int64 i = val.getAsBigInt();
            if (i <= 0 || i > 9) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9",
                    OPT_FRAC_PRECISION);
                return -1;
            }
            options.frPrec = i;
        } else if (val.getType() == NT_STRING) {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            long int num = strtol(tstr->c_str(), 0, 10);
            if (num <= 0 || num > 9) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9",
                    OPT_FRAC_PRECISION);
                return -1;
            }
            options.frPrec = num;
        } else {
            xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9",
                OPT_FRAC_PRECISION);
            return -1;
        }
    } else {
        xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9",
            OPT_FRAC_PRECISION);
        return -1;
    }

    return 0;
}

int ODBCConnection::setLoginTimeoutOption(QoreValue val, ExceptionSink* xsink) {
    if (val) {
        if (val.getType() == NT_INT) {
            int64 i = val.getAsBigInt();
            if (i < 0) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                    OPT_LOGIN_TIMEOUT);
                return -1;
            }
            options.loginTimeout = i;
        } else if (val.getType() == NT_STRING) {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            const char* buf = tstr->c_str();
            for (int i = 0, limit = tstr->size(); i < limit; i++) {
                if (!isdigit(buf[i]) && buf[i] != ' ')
                    xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                        OPT_LOGIN_TIMEOUT);
            }
            long int num = strtol(tstr->c_str(), 0, 10);
            if (num < 0) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                    OPT_LOGIN_TIMEOUT);
                return -1;
            }
            options.loginTimeout = num;
        } else {
            xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                OPT_LOGIN_TIMEOUT);
            return -1;
        }
    } else {
        xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
            OPT_LOGIN_TIMEOUT);
        return -1;
    }

    return 0;
}

int ODBCConnection::setConnectionTimeoutOption(QoreValue val, ExceptionSink* xsink) {
    if (val) {
        if (val.getType() == NT_INT) {
            int64 i = val.getAsBigInt();
            if (i < 0) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                    OPT_CONN_TIMEOUT);
                return -1;
            }
            options.connTimeout = i;
        } else if (val.getType() == NT_STRING) {
            const QoreStringNode* str = val.get<const QoreStringNode>();
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            const char* buf = tstr->c_str();
            for (int i = 0, limit = tstr->size(); i < limit; i++) {
                if (!isdigit(buf[i]) && buf[i] != ' ')
                    xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                        OPT_CONN_TIMEOUT);
            }
            long int num = strtol(tstr->c_str(), 0, 10);
            if (num < 0) {
                xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                    OPT_CONN_TIMEOUT);
                return -1;
            }
            options.connTimeout = num;
        } else {
            xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
                OPT_CONN_TIMEOUT);
            return -1;
        }
    } else {
        xsink->raiseException("ODBC-OPTION-ERROR", "'%s' option requires an integer argument from 0 up",
            OPT_CONN_TIMEOUT);
        return -1;
    }

    return 0;
}

int ODBCConnection::prepareConnectionString(ExceptionSink* xsink) {
    {
        const std::string& db = ds->getDBNameStr();
        if (!db.empty()) {
            if (db.find('=') != std::string::npos) {
                connStr.set(db);
                if (db.back() != ';') {
                    connStr.concat(';');
                }
            } else {
                connStr.sprintf("DSN=%s;", db.c_str());
            }
        }
    }

    if (ds->getUsername()) {
        connStr.sprintf("USER=%s;UID=%s;", ds->getUsername(), ds->getUsername());
    }

    if (ds->getPassword()) {
        connStr.sprintf("PASSWORD=%s;PWD=%s;", ds->getPassword(), ds->getPassword());
    }

    ConstHashIterator hi(ds->getConnectOptions());
    while (hi.next()) {
        QoreValue val = hi.get();
        if (!val) {
            continue;
        }

        // Skip module-specific (non-ODBC) options.
        if (!strcasecmp(DBI_OPT_NUMBER_OPT, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(DBI_OPT_NUMBER_STRING, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(DBI_OPT_NUMBER_NUMERIC, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_BIGINT_NATIVE, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_BIGINT_STRING, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_FRAC_PRECISION, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_QORE_TIMEZONE, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_LOGIN_TIMEOUT, hi.getKey())) {
            continue;
        }
        if (!strcasecmp(OPT_CONN_TIMEOUT, hi.getKey())) {
            continue;
        }

        // Append options to the connection string.
        qore_type_t ntype = val.getType();
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* strNode = val.get<const QoreStringNode>();
                TempEncodingHelper tstr(strNode, QCS_UTF8, xsink);
                if (*xsink) {
                    return -1;
                }
                if (!strcmp(hi.getKey(), "conn")) {
                    connStr.concat(tstr->c_str());
                } else {
                    QoreString key(hi.getKey());
                    key.toupr();
                    if (key.equal("DRIVER")) {
                        connStr.sprintf("%s={%s};", key.c_str(), tstr->c_str());
                    } else {
                        connStr.sprintf("%s=%s;", key.c_str(), tstr->c_str());
                    }
                }
                break;
            }
            case NT_INT:
            case NT_FLOAT:
            case NT_NUMBER: {
                QoreStringValueHelper vh(val);
                connStr.sprintf("%s=%s;", hi.getKey(), vh->c_str());
                break;
            }
            case NT_BOOLEAN: {
                bool b = val.getAsBool();
                connStr.sprintf("%s=%s;", hi.getKey(), b ? "1" : "0");
                break;
            }
            default: {
                xsink->raiseException("ODBC-OPTION-ERROR", "option values of type '%s' are not supported by the "
                    "ODBC driver", val.getTypeName());
                return -1;
            }
        } //  switch
    } // while

    return 0;
}

void ODBCConnection::getVersions() {
    SQLRETURN ret;
    SQLSMALLINT unused;

    char str[256];

    // Get DBMS (server) product name
    {
        ret = SQLGetInfoA(dbc, SQL_DBMS_NAME, str, 256, &unused);
        if (SQL_SUCCEEDED(ret)) {
            serverProductName = str;
        }
    }

    // Get DBMS (server) version
    {
        ret = SQLGetInfoA(dbc, SQL_DBMS_VER, str, 256, &unused);
        if (SQL_SUCCEEDED(ret)) {
            serverVersionString = str;
            serverVer = parseOdbcVersion(str);
        }
    }

    // Get DB name
    {
        ret = SQLGetInfoA(dbc, SQL_DATABASE_NAME, str, 256, &unused);
        if (SQL_SUCCEEDED(ret)) {
            dbName = str;
        }
    }

    // Get ODBC DB driver version.
    ret = SQLGetInfoA(dbc, SQL_DRIVER_VER, str, 256, &unused);
    if (SQL_SUCCEEDED(ret)) {
        clientVer = parseOdbcVersion(str);
    }
}

void ODBCConnection::checkIfConnectionDead(ExceptionSink* xsink) {
    SQLULEN deadAttr = 0;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, reinterpret_cast<SQLPOINTER>(&deadAttr),
        sizeof(deadAttr), 0);
    if (SQL_SUCCEEDED(ret)) {
        if (deadAttr == SQL_CD_TRUE) // Connection is dead.
            isDead = true;
    } else { // error
        handleDbcError("ODBC-CONNECTION-ERROR", "could not find out if connection is alive", xsink);
    }
}

void ODBCConnection::handleDbcError(const char* err, const char* desc, ExceptionSink* xsink) {
    if (!xsink) {
        return;
    }
    std::string s(desc);
    ODBCErrorHelper::extractDiag(SQL_HANDLE_DBC, dbc, s);
    xsink->raiseException(err, s.c_str());
}

void ODBCConnection::deadConnectionError(ExceptionSink* xsink) {
    if (!xsink)
        return;
    xsink->raiseException("ODBC-CONNECTION-DEAD-ERROR", "the connection is dead; create a new one");
}

// Version string is in the form "INT.INT.INT".
int ODBCConnection::parseOdbcVersion(const char* str) {
    int major = 0;
    int minor = 0;
    int sub = 0;
    int ret = sscanf(str, "%d.%d.%d", &major, &minor, &sub);
    if (ret == EOF || ret == 0) {
        return 0;
    }

    return major*1000000 + minor*10000 + sub;
}

} // namespace odbc
