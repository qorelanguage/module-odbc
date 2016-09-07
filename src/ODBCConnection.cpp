/* -*- indent-tabs-mode: nil -*- */
/*
  ODBCConnection.cpp

  Qore ODBC module

  Copyright (C) 2016 Ondrej Musil

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

#include <memory>

#include "qore/QoreLib.h"

#include "ErrorHelper.h"
#include "ODBCStatement.h"


ODBCConnection::ODBCConnection(Datasource* d, ExceptionSink* xsink) :
    ds(d),
    connected(false),
    clientVer(0),
    serverVer(0)
{
    SQLRETURN ret;
    // Allocate an environment handle.
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("DBI:ODBC:ENV-HANDLE-ERROR", "could not allocate an environment handle");
        return;
    }

    // Use ODBC version 3.
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*) SQL_OV_ODBC3, 0);

    // Allocate a connection handle.
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbConn);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("DBI:ODBC:CONNECTION-HANDLE-ERROR", "could not allocate a connection handle");
        return;
    }

    // Set connection attributes.
    SQLSetConnectAttr(dbConn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbConn, SQL_ATTR_QUIET_MODE, NULL, SQL_IS_POINTER);
    SQLSetConnectAttr(dbConn, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)60, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbConn, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)60, SQL_IS_UINTEGER);

    // Create ODBC connection string.
    QoreString connStr(QEM.findCreate("ASCII"));
    if(prepareConnectionString(connStr, xsink))
        return;

    SQLCHAR* odbcDS = reinterpret_cast<SQLCHAR*>(const_cast<char*>(connStr.getBuffer()));

    // Connect.
    ret = SQLDriverConnectA(dbConn, NULL, odbcDS, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("could not connect to the datasource; connection string: '%s'");
        ErrorHelper::extractDiag(SQL_HANDLE_DBC, dbConn, s);
        xsink->raiseException("DBI:ODBC:CONNECTION-ERROR", s.c_str(), connStr.getBuffer());
        return;
    }
    connected = true;

    // Get DBMS (server) version.
    char verStr[128]; // Will contain ver in the form "01.02.0034"
    SQLSMALLINT unused;
    ret = SQLGetInfoA(dbConn, SQL_DBMS_VER, verStr, 128, &unused);
    if (SQL_SUCCEEDED(ret)) {
        serverVer = parseOdbcVersion(verStr);
    }

    // Get ODBC DB driver version.
    ret = SQLGetInfoA(dbConn, SQL_DRIVER_VER, verStr, 128, &unused);
    if (SQL_SUCCEEDED(ret)) {
        clientVer = parseOdbcVersion(verStr);
    }

    // timezones, encoding
    //  ???

    ds->setDBEncoding("UTF-16");
}

ODBCConnection::~ODBCConnection() {
    while (connected) {
        SQLRETURN ret = SQLDisconnect(dbConn);
        if (SQL_SUCCEEDED(ret))
            break;
        qore_usleep(50*1000); // Sleep in intervals of 50 ms until disconnected.
    }

    // Free up allocated handles.
    if (dbConn != SQL_NULL_HDBC)
        SQLFreeHandle(SQL_HANDLE_DBC, dbConn);
    if (env != SQL_NULL_HENV)
        SQLFreeHandle(SQL_HANDLE_ENV, env);
}

int ODBCConnection::commit(ExceptionSink* xsink) {
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, dbConn, SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("DBI:ODBC:COMMIT-ERROR", "could not commit the transaction", xsink);
        return -1;
    }
    return 0;
}

int ODBCConnection::rollback(ExceptionSink* xsink) {
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, dbConn, SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("DBI:ODBC:ROLLBACK-ERROR", "could not rollback the transaction", xsink);
        return -1;
    }
    return 0;
}

QoreListNode* ODBCConnection::selectRows(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (res.exec(qstr, args, xsink))
        return NULL;

    return res.getOutputList(xsink);
}

QoreHashNode* ODBCConnection::selectRow(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (res.exec(qstr, args, xsink))
        return NULL;

    return res.getSingleRow(xsink);
}

AbstractQoreNode* ODBCConnection::exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);
    if (res.exec(qstr, args, xsink))
        return NULL;

    if (res.hasResultData())
        return res.getOutputHash(xsink);

    return new QoreBigIntNode(res.rowsAffected());
}

AbstractQoreNode* ODBCConnection::execRaw(const QoreString* qstr, ExceptionSink* xsink) {
    ODBCStatement res(this, xsink);

    // Convert string to required character encoding or copy.
    std::unique_ptr<QoreString> str(qstr->convertEncoding(QEM.findCreate("ASCII"), xsink));
    if (!str.get())
        return NULL;

    if (res.exec(str->getBuffer(), xsink))
        return NULL;

    if (res.hasResultData())
        return res.getOutputHash(xsink);

    return new QoreBigIntNode(res.rowsAffected());
}

void ODBCConnection::allocStatementHandle(SQLHSTMT& stmt, ExceptionSink* xsink) {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbConn, &stmt);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("DBI:ODBC:STATEMENT-ALLOC-ERROR", "could not allocate a statement handle", xsink);
    }
}

void ODBCConnection::handleDbcError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::string s(desc);
    ErrorHelper::extractDiag(SQL_HANDLE_DBC, dbConn, s);
    xsink->raiseException(err, s.c_str());
}

int ODBCConnection::prepareConnectionString(QoreString& str, ExceptionSink* xsink) {
    if (ds->getDBName() && strlen(ds->getDBName()) > 0)
        str.sprintf("DSN=%s;", ds->getDBName());

    if (ds->getUsername())
        str.sprintf("UID=%s;", ds->getUsername());

    if (ds->getPassword())
        str.sprintf("PWD=%s;", ds->getPassword());

    ConstHashIterator hi(ds->getConnectOptions());
    while (hi.next()) {
        const AbstractQoreNode* val = hi.getValue();
        if (!val)
            continue;

        qore_type_t ntype = val->getType();
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* strNode = reinterpret_cast<const QoreStringNode*>(val);
                TempEncodingHelper tstr(strNode, QEM.findCreate("ASCII"), xsink);
                if (*xsink)
                    return -1;
                std::unique_ptr<QoreString> key(hi.getKeyString());
                key->tolwr();
                if (key->equal("driver")) {
                    str.sprintf("%s={%s};", hi.getKey(), tstr->getBuffer());
                }
                else {
                    str.sprintf("%s=%s;", hi.getKey(), tstr->getBuffer());
                }
                break;
            }
            case NT_INT:
            case NT_FLOAT:
            case NT_NUMBER: {
                QoreStringValueHelper vh(val);
                str.sprintf("%s=%s;", hi.getKey(), vh->getBuffer());
                break;
            }
            case NT_BOOLEAN: {
                bool b = reinterpret_cast<const QoreBoolNode*>(val)->getValue();
                str.sprintf("%s=%s;", hi.getKey(), b ? "1" : "0");
                break;
            }
            default: {
                xsink->raiseException("DBI:ODBC:OPTION-ERROR", "option values of type '%s' are not supported by the ODBC driver", val->getTypeName());
                return -1;
            }
        } //  switch
    } // while

    return 0;
}

// Version string is in the form "xx.xx.xxxx".
int ODBCConnection::parseOdbcVersion(const char* str) {
    int major, minor, sub;
    major = minor = sub = 0;

    major += (str[0] - 48) * 10;
    major += str[1] - 48;

    minor += (str[3] - 48) * 10;
    minor += str[4] - 48;

    sub += (str[6] - 48) * 1000;
    sub += (str[7] - 48) * 100;
    sub += (str[8] - 48) * 10;
    sub += str[9] - 48;

    return major*1000000 + minor*10000 + sub;
}

