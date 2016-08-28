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

#include "ErrorHelper.h"
#include "ODBCStatement.h"


ODBCConnection::ODBCConnection(Datasource* d, const char* str, ExceptionSink* xsink) : ds(d) {
    SQLRETURN ret;
    // Allocate an environment handle.
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("DBI:ODBC:ENV-HANDLE-ERROR", "could not allocate an environment handle", xsink);
        return;
    }

    // We want ODBC 3 support.
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*) SQL_OV_ODBC3, 0);

    // Allocate a connection handle.
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbConn);
    if (!SQL_SUCCEEDED(ret)) { // error
        xsink->raiseException("DBI:ODBC:CONNECTION-HANDLE-ERROR", "could not allocate a connection handle", xsink);
        return;
    }

    // Set connection attributes.
    SQLSetConnectAttr(dbConn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
    SQLSetConnectAttr(dbConn, SQL_ATTR_QUIET_MODE, NULL, SQL_IS_POINTER);

    SQLCHAR* odbcDS = reinterpret_cast<SQLCHAR*>(const_cast<char*>(str));

    // Connect.
    //ret = SQLDriverConnectA(dbConn, NULL, "DSN=web;", SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    ret = SQLDriverConnectA(dbConn, NULL, odbcDS, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("could not connect to the driver", "DBI:ODBC:CONNECTION-ERROR", xsink);
        return;
    }

    // server version

    // timezones

    // encoding

    /*const char *pstr;
    // get server version to encode/decode binary values properly
#if POSTGRES_VERSION_MAJOR >= 8
    int server_version = PQserverVersion(pc);
    //printd(5, "version: %d\n", server_version);
    interval_has_day = server_version >= 80100 ? true : false;
#else
    pstr = PQparameterStatus(pc, "server_version");
    interval_has_day = strcmp(pstr, "8.1") >= 0 ? true : false;
#endif
    pstr = PQparameterStatus(pc, "integer_datetimes");

    if (!pstr || !pstr[0]) {
        // encoding does not matter here; we are only getting an integer
        QorePgsqlStatement res(this, QCS_DEFAULT);
        integer_datetimes = res.checkIntegerDateTimes(xsink);
    }
    else
        integer_datetimes = strcmp(pstr, "off");

    if (PQsetClientEncoding(pc, ds->getDBEncoding()))
        xsink->raiseException("DBI:PGSQL:ENCODING-ERROR", "invalid PostgreSQL encoding '%s'", ds->getDBEncoding());*/
}

ODBCConnection::~ODBCConnection() {
    // Free up allocated handles.
    SQLFreeHandle(SQL_HANDLE_DBC, dbConn);
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

int ODBCConnection::getServerVersion() const {
    // TODO
    return 0;
}

void ODBCConnection::allocStatementHandle(SQLHSTMT& stmt, ExceptionSink* xsink) {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbConn, &stmt);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleDbcError("DBI:ODBC:STATEMENT-ALLOC-ERROR", "could not allocate a statement handle", xsink);
    }
}

void ODBCConnection::handleDbcError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::stringstream s(desc);
    ErrorHelper::extractDiag(SQL_HANDLE_DBC, dbConn, s);
    xsink->raiseException(err, s.str().c_str());
}

