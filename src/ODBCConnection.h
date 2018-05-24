/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
    ODBCConnection.h

    Qore ODBC module

    Copyright (C) 2016 - 2018 Qore Technologies s.r.o.

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

#ifndef _QORE_MODULE_ODBC_ODBCCONNECTION_H
#define _QORE_MODULE_ODBC_ODBCCONNECTION_H

#include <memory>

#include <sql.h>
#include <sqlext.h>

#include "qore/Qore.h"

#include "ODBCOptions.h"

class AbstractQoreZoneInfo;

namespace odbc {

//! A class representing an ODBC connection.
class ODBCConnection {
public:
    //! Constructor.
    /** @param d Qore datasource
        @param xsink exception sink
     */
    DLLLOCAL ODBCConnection(Datasource* d, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL ~ODBCConnection();

    //! Disabled copy constructor.
    DLLLOCAL ODBCConnection(const ODBCConnection& c) = delete;

    //! Disabled assignment operator.
    DLLLOCAL ODBCConnection& operator=(const ODBCConnection& c) = delete;

    //! Connect to the server.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int connect(ExceptionSink* xsink);

    //! Disconnect the connection.
    DLLLOCAL void disconnect();

    //! Commit an ODBC transaction.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int commit(ExceptionSink* xsink);

    //! Rollback an ODBC transaction.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int rollback(ExceptionSink* xsink);

    //! Select from the database.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL QoreValue select(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

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
    DLLLOCAL QoreValue exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute a raw SQL statement.
    /** @param qstr SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL QoreValue execRaw(const QoreString* qstr, ExceptionSink* xsink);

    //! Allocate an ODBC statement handle.
    /** @param stmt ODBC statement handle
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int allocStatementHandle(SQLHSTMT& stmt, ExceptionSink* xsink);

    //! Set an option for the connection.
    /** @param opt option name
        @param val option value to use
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int setOption(const char* opt, const QoreValue val, ExceptionSink* xsink);

    //! Get the current value of an option of the connection.
    /** @param opt option name

        @return option's value
     */
    DLLLOCAL QoreValue getOption(const char* opt);

    //! Get the current options of the connection.
    DLLLOCAL ODBCOptions getOptions() const { return options; }

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

    //! Return datasource of the connection.
    DLLLOCAL Datasource* getDatasource() const {
        return ds;
    }

    //! Return server timezone set for this connection.
    DLLLOCAL const AbstractQoreZoneInfo* getServerTimezone() const {
        return serverTz;
    }

private:
    //! Qore datasource.
    Datasource* ds;

    //! Server timezone.
    const AbstractQoreZoneInfo* serverTz = nullptr;

    //! ODBC environment handle.
    SQLHENV env = SQL_NULL_HENV;

    //! ODBC connection handle.
    SQLHDBC dbc = SQL_NULL_HDBC;

    //! Connection string.
    QoreString connStr;

    //! UTF-16 connection string.
    std::unique_ptr<QoreString> connStrUTF16;

    //! Whether an ODBC connection has been opened.
    bool connected = false;

    //! Whether the ODBC connection is dead.
    bool isDead = false;

    //! Whether a transaction is active.
    bool activeTransaction = false;

    //! Options regarding parameters and results.
    ODBCOptions options;

    //! Version of the used ODBC DB driver.
    int clientVer = 0;

    //! Version of the connected DBMS.
    int serverVer = 0;

    //! Initialize environment.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int envInit(ExceptionSink* xsink);

    //! Parse options passed through Datasource.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int parseOptions(ExceptionSink* xsink);

    //! Set fractional precision option to the passed value, check for errors.
    /** @param val new value for the option
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int setFracPrecisionOption(QoreValue val, ExceptionSink* xsink);

    //! Set login timeout to the passed value, check for errors.
    /** @param val new value for the option
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int setLoginTimeoutOption(QoreValue val, ExceptionSink* xsink);

    //! Set connection timeout to the passed value, check for errors.
    /** @param val new value for the option
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int setConnectionTimeoutOption(QoreValue val, ExceptionSink* xsink);

    //! Prepare ODBC connection string and save it to the passed string.
    /** @param str connection string
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int prepareConnectionString(ExceptionSink* xsink);

    // Get DBMS (server) and ODBC DB driver (client) versions.
    DLLLOCAL void getVersions();

    //! Check if connection is dead.
    /** Checks if the ODBC connection is dead and optionally modifies isDead variable.
        @param xsink exception sink
     */
    DLLLOCAL void checkIfConnectionDead(ExceptionSink* xsink);

    //! Extract ODBC diagnostic and raise a Qore exception.
    /** @param err error "code"
        @param desc error description
        @param xsink exception sink
     */
    DLLLOCAL void handleDbcError(const char* err, const char* desc, ExceptionSink* xsink);

    //! Throw a CONNECTION-DEAD-ERROR exception via xsink.
    DLLLOCAL void deadConnectionError(ExceptionSink* xsink);

    //! Parse ODBC version string.
    /** @param str version string

        @return version in the form: major*1000000 + minor*10000 + sub
     */
    DLLLOCAL int parseOdbcVersion(const char* str);
};

} // namespace odbc

#endif // _QORE_MODULE_ODBC_ODBCCONNECTION_H

