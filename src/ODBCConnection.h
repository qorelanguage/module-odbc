/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCConnection.h

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

#ifndef _QORE_ODBCCONNECTION_H
#define _QORE_ODBCCONNECTION_H

#include <sql.h>
#include <sqlext.h>

#include "qore/AbstractPrivateData.h"
#include "qore/QoreEncoding.h"
#include "qore/QoreString.h"
#include "qore/QoreStringNode.h"
#include "qore/AbstractQoreNode.h"
#include "qore/QoreHashNode.h"
#include "qore/QoreListNode.h"
#include "qore/common.h"
#include "qore/Datasource.h"
#include "qore/ExceptionSink.h"
#include "qore/QoreBigIntNode.h"
#include "qore/QoreListNode.h"


//! A class representing an ODBC connection.
class ODBCConnection {
private:
    //! Qore datasource.
    Datasource* ds;

    //! ODBC environment handle.
    SQLHENV env;

    //! ODBC connection handle.
    SQLHDBC dbConn;

    //! Version of the used ODBC DB driver.
    int clientVer;

    //! Version of the connected DBMS.
    int serverVer;

    //! Extract ODBC diagnostic and raise a Qore exception.
    /** @param err error "code"
        @param desc error description
        @param xsink exception sink
     */
    void handleDbcError(const char* err, const char* desc, ExceptionSink *xsink);

    //! Parse ODBC version string.
    /** @param str version string

        @return version in the form: major*1000000 + minor*10000 + sub
     */
    int parseOdbcVersion(const char* str);

public:
    //! Constructor.
    /** @param d Qore datasource
        @param str connection string
        @param xsink exception sink
     */
    DLLLOCAL ODBCConnection(Datasource* d, const char* str, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL ~ODBCConnection();

    //! Commit an ODBC transaction.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int commit(ExceptionSink* xsink);

    //! Rollback an ODBC transaction.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int rollback( ExceptionSink* xsink);

    //! Select multiple rows from the database.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return a list of row hashes
     */
    DLLLOCAL QoreListNode* selectRows(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Select one row from the database.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return one row hash
     */
    DLLLOCAL QoreHashNode* selectRow(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute a Qore-style SQL statement with arguments.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL AbstractQoreNode* exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute a raw SQL statement.
    /** @param qstr SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL AbstractQoreNode* execRaw(const QoreString* qstr, ExceptionSink* xsink);

    //! Allocate an ODBC statement handle.
    /** @param stmt ODBC statement handle
        @param xsink exception sink
     */
    DLLLOCAL void allocStatementHandle(SQLHSTMT& stmt, ExceptionSink* xsink);

    //! Return ODBC driver (client) version.
    /** @return version in the form: major*1000000 + minor*10000 + sub
     */
    DLLLOCAL int getClientVersion() const {
        return clientVer;
    }

    //! Return DBMS (server) version.
    /** @return version in the form: major*1000000 + minor*10000 + sub
     */
    DLLLOCAL int getServerVersion() const {
        return serverVer;
    }

    DLLLOCAL Datasource* getDs() const {
        return ds;
    }

    DLLLOCAL bool wasInTransaction() const {
        return ds->activeTransaction();
    }
};

#endif // _QORE_ODBCCONNECTION_H

