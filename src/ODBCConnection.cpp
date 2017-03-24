/* -*- indent-tabs-mode: nil -*- */
/*
  ODBCConnection.cpp

  Qore ODBC module

  Copyright (C) 2016 Qore Technologies s.r.o.

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

static SQLINTEGER getUTF8CharCount(const char* str) {
    SQLINTEGER len = 0;
    for (; *str; ++str) if ((*str & 0xC0) != 0x80) ++len;
    return len;
}

ODBCConnection::ODBCConnection(Datasource* d, ExceptionSink* xsink) :
    ds(d),
    serverTz(0),
    env(SQL_NULL_HENV),
    dbc(SQL_NULL_HDBC),
    connStr(QCS_UTF8),
    connected(false),
    isDead(false),
    activeTransaction(false),
    clientVer(0),
    serverVer(0)
{
    // Parse options passed through the datasource.
    if (parseOptions(xsink))
        return;

    // Timezone handling.
    if (!serverTz) {
        DateTime d(false); // Used just to get the local zone info.
        serverTz = d.getZone();
    }

    // Initialize environment.
    if (envInit(xsink))
        return;

    // Prepare ODBC connection string.
    if(prepareConnectionString(xsink))
        return;

    // Connect.
    if (connect(xsink))
        return;

    // Get DBMS (server) and ODBC DB driver (client) versions.
    getVersions();
}

ODBCConnection::~ODBCConnection() {
    disconnect();

    // Free up allocated handles.
    if (dbc != SQL_NULL_HDBC)
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    if (env != SQL_NULL_HENV)
        SQLFreeHandle(SQL_HANDLE_ENV, env);
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
        xsink->raiseException("DBI:ODBC:CONNECTION-HANDLE-ERROR", "could not allocate a connection handle");
        return -1;
    }

    // Set connection attributes.
    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbc, SQL_ATTR_QUIET_MODE, 0, SQL_IS_POINTER);
    SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)options.loginTimeout, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)options.connTimeout, SQL_IS_UINTEGER);

    // Get connection string.
    SQLWCHAR* odbcDS = reinterpret_cast<SQLWCHAR*>(const_cast<char*>(connStrUTF16->getBuffer()));

    // Connect.
    ret = SQLDriverConnectW(dbc, 0, odbcDS, getUTF8CharCount(connStr.getBuffer()), 0, 0, 0, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("could not connect to the datasource; connection string: '%s'");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_DBC, dbc, s);
        xsink->raiseException("DBI:ODBC:CONNECTION-ERROR", s.c_str(), connStr.getBuffer());
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
        }
        else {
            char state[7];
            memset(state, 0, sizeof(state));
            ODBCErrorHelper::extractState(SQL_HANDLE_DBC, dbc, state);
            //fprintf(stderr, "state: %s\n", state);
            if (strncmp(state, "08003", 5) == 0) { // Connection not open.
                connected = false;
            }
            else if (strncmp(state, "HY000", 5) == 0) { // General error.
                connected = false;
            }
            else if (strncmp(state, "HYT01", 5) == 0) { // Connection timeout expired.
                connected = false;
            }
            else if (strncmp(state, "HY117", 5) == 0) { // Connection is suspended due to unknown transaction state. Only disconnect and read-only functions are allowed.
                connected = false;
            }
            else if (strncmp(state, "25000", 5) == 0) { // Transaction still active.
                if (!activeTransaction) {
                    if (isDead)
                        isDead = false;
                    rollback(0);
                }
            }
            else {
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
        handleDbcError("DBI:ODBC:COMMIT-ERROR", "could not commit the transaction", xsink);
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
        handleDbcError("DBI:ODBC:ROLLBACK-ERROR", "could not rollback the transaction", xsink);
        return -1;
    }
    activeTransaction = false;
    return 0;
}

AbstractQoreNode* ODBCConnection::select(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return 0;

    if (res.exec(qstr, args, xsink))
        return 0;

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return new QoreBigIntNode(res.rowsAffected());
}

QoreListNode* ODBCConnection::selectRows(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return 0;

    if (res.exec(qstr, args, xsink))
        return 0;

    return res.getOutputList(xsink);
}

QoreHashNode* ODBCConnection::selectRow(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (*xsink)
        return 0;

    if (res.exec(qstr, args, xsink))
        return 0;

    return res.getSingleRow(xsink);
}

AbstractQoreNode* ODBCConnection::exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    //fprintf(stderr, "ODBCConnection::exec called: '%s'\n", qstr->c_str());
    ODBCStatement res(this, xsink);
    if (*xsink)
        return 0;

    if (res.exec(qstr, args, xsink)) {
        //fprintf(stderr, "exec failed\n");
        return 0;
    }
    activeTransaction = true;

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return new QoreBigIntNode(res.rowsAffected());
}

AbstractQoreNode* ODBCConnection::execRaw(const QoreString* qstr, ExceptionSink* xsink) {
    //fprintf(stderr, "ODBCConnection::execRaw called: '%s'\n", qstr->c_str());
    ODBCStatement res(this, xsink);
    if (*xsink)
        return 0;

    if (res.exec(qstr, xsink)) {
        //fprintf(stderr, "execRaw failed\n");
        return 0;
    }
    activeTransaction = true;

    if (res.hasResultData())
        return res.getOutputHash(xsink, false);

    return new QoreBigIntNode(res.rowsAffected());
}

int ODBCConnection::setOption(const char* opt, const AbstractQoreNode* val, ExceptionSink* xsink) {
    if (!strcasecmp(opt, DBI_OPT_NUMBER_OPT)) {
        options.numeric = ENO_OPTIMAL;
        return 0;
    }
    else if (!strcasecmp(opt, DBI_OPT_NUMBER_STRING)) {
        options.numeric = ENO_STRING;
        return 0;
    }
    else if (!strcasecmp(opt, DBI_OPT_NUMBER_NUMERIC)) {
        options.numeric = ENO_NUMERIC;
        return 0;
    }
    else if (!strcasecmp(opt, OPT_BIGINT_NATIVE)) {
        options.bigint = EBO_NATIVE;
        return 0;
    }
    else if (!strcasecmp(opt, OPT_BIGINT_STRING)) {
        options.bigint = EBO_STRING;
        return 0;
    }
    else if (!strcasecmp(opt, OPT_FRAC_PRECISION)) {
        return setFracPrecisionOption(val, xsink);
    }
    else if (!strcasecmp(opt, OPT_QORE_TIMEZONE)) {
        if (val && val->getType() == NT_STRING) {
            const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(val);
            TempEncodingHelper tzName(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            serverTz = find_create_timezone(tzName->getBuffer(), xsink);
            if (*xsink)
                return -1;
        }
        else {
            xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires a name of a timezone (e.g. \"Europe/Prague\") or a time offset string (e.g. \"+02:00\")", OPT_QORE_TIMEZONE);
            return -1;
        }
    }
    else if (!strcasecmp(opt, OPT_LOGIN_TIMEOUT)) {
        return setLoginTimeoutOption(val, xsink);
    }
    else if (!strcasecmp(opt, OPT_CONN_TIMEOUT)) {
        return setConnectionTimeoutOption(val, xsink);
    }

    return 0;
}

AbstractQoreNode* ODBCConnection::getOption(const char* opt) {
    assert(options.numeric == ENO_OPTIMAL || options.numeric == ENO_STRING || options.numeric == ENO_NUMERIC);
    assert(options.bigint == EBO_NATIVE || options.bigint == EBO_STRING);

    if (!strcasecmp(opt, DBI_OPT_NUMBER_OPT))
        return get_bool_node(options.numeric == ENO_OPTIMAL);

    if (!strcasecmp(opt, DBI_OPT_NUMBER_STRING))
        return get_bool_node(options.numeric == ENO_STRING);

    if (!strcasecmp(opt, DBI_OPT_NUMBER_NUMERIC))
        return get_bool_node(options.numeric == ENO_NUMERIC);

    if (!strcasecmp(opt, OPT_BIGINT_NATIVE))
        return get_bool_node(options.bigint == EBO_NATIVE);

    if (!strcasecmp(opt, OPT_BIGINT_STRING))
        return get_bool_node(options.bigint == EBO_STRING);

    if (!strcasecmp(opt, OPT_FRAC_PRECISION))
        return new QoreBigIntNode(static_cast<int64>(options.frPrec));

    if (!strcasecmp(opt, OPT_QORE_TIMEZONE)) {
        if (serverTz) {
            DateTime x(static_cast<int64>(360000));
            x.setZone(serverTz);
            qore_tm info;
            x.getInfo(info);
            return new QoreStringNode(info.regionName());
        }
        else {
            return new QoreStringNode("UTC");
        }
    }

    if (!strcasecmp(opt, OPT_LOGIN_TIMEOUT))
        return new QoreBigIntNode(static_cast<int64>(options.connTimeout));

    if (!strcasecmp(opt, OPT_CONN_TIMEOUT))
        return new QoreBigIntNode(static_cast<int64>(options.loginTimeout));

    return 0;
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
        handleDbcError("DBI:ODBC:STATEMENT-ALLOC-ERROR", "could not allocate a statement handle", xsink);
        return -1;
    }

    return 0;
}

int ODBCConnection::envInit(ExceptionSink* xsink) {
    SQLRETURN ret;
    // Allocate an environment handle.
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("DBI:ODBC:ENV-HANDLE-ERROR", "could not allocate an environment handle");
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
            const AbstractQoreNode* val = hi.getValue();
            if (setFracPrecisionOption(val, xsink))
                return -1;
            continue;
        }
        if (!strcasecmp(OPT_QORE_TIMEZONE, hi.getKey())) {
            const AbstractQoreNode* val = hi.getValue();
            if (val->getType() != NT_STRING) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "non-string value passed for the '%s' option", OPT_QORE_TIMEZONE);
                return -1;
            }
            const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(val);
            const AbstractQoreZoneInfo* tz = find_create_timezone(str->getBuffer(), xsink);
            if (*xsink)
                return -1;
            serverTz = tz;
            continue;
        }
        if (!strcasecmp(OPT_LOGIN_TIMEOUT, hi.getKey())) {
            const AbstractQoreNode* val = hi.getValue();
            if (setLoginTimeoutOption(val, xsink))
                return -1;
            continue;
        }
        if (!strcasecmp(OPT_CONN_TIMEOUT, hi.getKey())) {
            const AbstractQoreNode* val = hi.getValue();
            if (setConnectionTimeoutOption(val, xsink))
                return -1;
            continue;
        }
    }
    return 0;
}

int ODBCConnection::setFracPrecisionOption(const AbstractQoreNode* val, ExceptionSink* xsink) {
    if (val) {
        if (val->getType() == NT_INT) {
            const QoreBigIntNode* in = reinterpret_cast<const QoreBigIntNode*>(val);
            if (in->val <= 0 || in->val > 9) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9", OPT_FRAC_PRECISION);
                return -1;
            }
            options.frPrec = in->val;
        }
        else if (val->getType() == NT_STRING) {
            const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(val);
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            long int num = strtol(tstr->getBuffer(), 0, 10);
            if (num <= 0 || num > 9) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9", OPT_FRAC_PRECISION);
                return -1;
            }
            options.frPrec = num;
        }
        else {
            xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9", OPT_FRAC_PRECISION);
            return -1;
        }
    }
    else {
        xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 1 to 9", OPT_FRAC_PRECISION);
        return -1;
    }

    return 0;
}

int ODBCConnection::setLoginTimeoutOption(const AbstractQoreNode* val, ExceptionSink* xsink) {
    if (val) {
        if (val->getType() == NT_INT) {
            const QoreBigIntNode* in = reinterpret_cast<const QoreBigIntNode*>(val);
            if (in->val < 0) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_LOGIN_TIMEOUT);
                return -1;
            }
            options.loginTimeout = in->val;
        }
        else if (val->getType() == NT_STRING) {
            const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(val);
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            const char* buf = tstr->getBuffer();
            for (int i = 0, limit = tstr->size(); i < limit; i++) {
                if (!isdigit(buf[i]) && buf[i] != ' ')
                    xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_LOGIN_TIMEOUT);
            }
            long int num = strtol(tstr->getBuffer(), 0, 10);
            if (num < 0) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_LOGIN_TIMEOUT);
                return -1;
            }
            options.loginTimeout = num;
        }
        else {
            xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_LOGIN_TIMEOUT);
            return -1;
        }
    }
    else {
        xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_LOGIN_TIMEOUT);
        return -1;
    }

    return 0;
}

int ODBCConnection::setConnectionTimeoutOption(const AbstractQoreNode* val, ExceptionSink* xsink) {
    if (val) {
        if (val->getType() == NT_INT) {
            const QoreBigIntNode* in = reinterpret_cast<const QoreBigIntNode*>(val);
            if (in->val < 0) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_CONN_TIMEOUT);
                return -1;
            }
            options.connTimeout = in->val;
        }
        else if (val->getType() == NT_STRING) {
            const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(val);
            TempEncodingHelper tstr(str, QCS_UTF8, xsink);
            if (*xsink)
                return -1;
            const char* buf = tstr->getBuffer();
            for (int i = 0, limit = tstr->size(); i < limit; i++) {
                if (!isdigit(buf[i]) && buf[i] != ' ')
                    xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_CONN_TIMEOUT);
            }
            long int num = strtol(tstr->getBuffer(), 0, 10);
            if (num < 0) {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_CONN_TIMEOUT);
                return -1;
            }
            options.connTimeout = num;
        }
        else {
            xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_CONN_TIMEOUT);
            return -1;
        }
    }
    else {
        xsink->raiseException("DBI:ODBC:OPTION-ERROR", "'%s' option requires an integer argument from 0 up", OPT_CONN_TIMEOUT);
        return -1;
    }

    return 0;
}

int ODBCConnection::prepareConnectionString(ExceptionSink* xsink) {
    if (ds->getDBName() && strlen(ds->getDBName()) > 0)
        connStr.sprintf("DSN=%s;", ds->getDBName());

    if (ds->getUsername())
        connStr.sprintf("UID=%s;", ds->getUsername());

    if (ds->getPassword())
        connStr.sprintf("PWD=%s;", ds->getPassword());

    ConstHashIterator hi(ds->getConnectOptions());
    while (hi.next()) {
        const AbstractQoreNode* val = hi.getValue();
        if (!val)
            continue;

        // Skip module-specific (non-ODBC) options.
        if (!strcasecmp(DBI_OPT_NUMBER_OPT, hi.getKey()))
            continue;
        if (!strcasecmp(DBI_OPT_NUMBER_STRING, hi.getKey()))
            continue;
        if (!strcasecmp(DBI_OPT_NUMBER_NUMERIC, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_BIGINT_NATIVE, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_BIGINT_STRING, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_FRAC_PRECISION, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_QORE_TIMEZONE, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_LOGIN_TIMEOUT, hi.getKey()))
            continue;
        if (!strcasecmp(OPT_CONN_TIMEOUT, hi.getKey()))
            continue;

        // Append options to the connection string.
        qore_type_t ntype = val->getType();
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* strNode = reinterpret_cast<const QoreStringNode*>(val);
                TempEncodingHelper tstr(strNode, QCS_UTF8, xsink);
                if (*xsink)
                    return -1;
                std::unique_ptr<QoreString> key(hi.getKeyString());
                key->tolwr();
                if (key->equal("driver")) {
                    connStr.sprintf("%s={%s};", hi.getKey(), tstr->getBuffer());
                }
                else {
                    connStr.sprintf("%s=%s;", hi.getKey(), tstr->getBuffer());
                }
                break;
            }
            case NT_INT:
            case NT_FLOAT:
            case NT_NUMBER: {
                QoreStringValueHelper vh(val);
                connStr.sprintf("%s=%s;", hi.getKey(), vh->getBuffer());
                break;
            }
            case NT_BOOLEAN: {
                bool b = reinterpret_cast<const QoreBoolNode*>(val)->getValue();
                connStr.sprintf("%s=%s;", hi.getKey(), b ? "1" : "0");
                break;
            }
            default: {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "option values of type '%s' are not supported by the ODBC driver", val->getTypeName());
                return -1;
            }
        } //  switch
    } // while

    // Create final UTF-16 connection string.
#ifdef WORDS_BIGENDIAN
    std::unique_ptr<QoreString> utf16Str(connStr.convertEncoding(QCS_UTF16BE, xsink));
#else
    std::unique_ptr<QoreString> utf16Str(connStr.convertEncoding(QCS_UTF16LE, xsink));
#endif
    if (*xsink || !utf16Str)
        return -1;

    connStrUTF16 = std::move(utf16Str);
    return 0;
}

void ODBCConnection::getVersions() {
    SQLRETURN ret;

    // Get DBMS (server) version.
    char verStr[128]; // Will contain ver in the form "01.02.0034"
    SQLSMALLINT unused;
    ret = SQLGetInfoA(dbc, SQL_DBMS_VER, verStr, 128, &unused);
    if (SQL_SUCCEEDED(ret)) {
        serverVer = parseOdbcVersion(verStr);
    }

    // Get ODBC DB driver version.
    ret = SQLGetInfoA(dbc, SQL_DRIVER_VER, verStr, 128, &unused);
    if (SQL_SUCCEEDED(ret)) {
        clientVer = parseOdbcVersion(verStr);
    }
}

void ODBCConnection::checkIfConnectionDead(ExceptionSink* xsink) {
    SQLULEN deadAttr = 0;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, reinterpret_cast<SQLPOINTER>(&deadAttr), sizeof(deadAttr), 0);
    if (SQL_SUCCEEDED(ret)) {
        if (deadAttr == SQL_CD_TRUE) // Connection is dead.
            isDead = true;
    }
    else { // error
        handleDbcError("DBI:ODBC:CONNECTION-ERROR", "could not find out if connection is alive", xsink);
    }
}

void ODBCConnection::handleDbcError(const char* err, const char* desc, ExceptionSink* xsink) {
    if (!xsink)
        return;
    std::string s(desc);
    ODBCErrorHelper::extractDiag(SQL_HANDLE_DBC, dbc, s);
    xsink->raiseException(err, s.c_str());
}

void ODBCConnection::deadConnectionError(ExceptionSink* xsink) {
    if (!xsink)
        return;
    xsink->raiseException("DBI:ODBC:CONNECTION-DEAD-ERROR", "the connection is dead; create a new one");
}

// Version string is in the form "INT.INT.INT".
int ODBCConnection::parseOdbcVersion(const char* str) {
    int major = 0;
    int minor = 0;
    int sub = 0;
    int ret = sscanf(str, "%d.%d.%d", &major, &minor, &sub);
    if (ret == EOF || ret == 0)
        return 0;

    return major*1000000 + minor*10000 + sub;
}

} // namespace odbc
