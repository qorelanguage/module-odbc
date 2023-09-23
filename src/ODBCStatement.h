/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
    ODBCStatement.h

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

#ifndef _QORE_MODULE_ODBC_ODBCSTATEMENT_H
#define _QORE_MODULE_ODBC_ODBCSTATEMENT_H

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <sql.h>
#include <sqlext.h>

#include <qore/Qore.h>

#include "ODBCErrorHelper.h"
#include "ODBCOptions.h"
#include "ODBCResultColumn.h"
#include "ParamArrayHolder.h"
#include "ParamHolder.h"

namespace odbc {

class ODBCConnection;

//! A class representing one ODBC SQL statement.
class ODBCStatement {
public:
    //! Constructor.
    DLLLOCAL ODBCStatement(ODBCConnection* c, ExceptionSink* xsink);

    //! Constructor.
    DLLLOCAL ODBCStatement(Datasource* ds, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL virtual ~ODBCStatement();

    //! Disabled copy constructor.
    DLLLOCAL ODBCStatement(const ODBCStatement& s) = delete;

    //! Disabled assignment operator.
    DLLLOCAL ODBCStatement& operator=(const ODBCStatement& s) = delete;

    //! Return how many rows were affected by the executed statement.
    /** @return count of affected rows; -1 in case the number is not available
     */
    DLLLOCAL int rowsAffected() const { return affectedRowCount; }

    //! Return if there are any results available.
    DLLLOCAL bool hasResultData();

    //! Return a hash describing result columns.
    /** @param xsink exception sink

        @return hash describing result columns
     */
    DLLLOCAL QoreHashNode* describe(ExceptionSink* xsink);

    //! Get result hash.
    /** @param xsink exception sink
        @oaram emptyHashIfNothing whether to return empty hash or empty hash with column names when no rows available
        @param maxRows maximum count of rows to return; if <= 0 the count of returned rows is not limited

        @return hash of result column lists
     */
    DLLLOCAL QoreHashNode* getOutputHash(ExceptionSink* xsink, bool emptyHashIfNothing, int maxRows = -1);

    //! Get result list.
    /** @param xsink exception sink
        @param maxRows maximum count of rows to return; if <= 0 the count of returned rows is not limited

        @return list of row hashes
     */
    DLLLOCAL QoreListNode* getOutputList(ExceptionSink* xsink, int maxRows = -1);

    //! Get one result row in the form of a hash.
    /** @param xsink exception sink

        @return one result-set row
     */
    DLLLOCAL QoreHashNode* getSingleRow(ExceptionSink* xsink);

    //! Execute a Qore-style SQL statement with arguments.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute an SQL statement.
    /** @param qstr raw SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int exec(const QoreString* qstr, ExceptionSink* xsink);

    //! Clear.
    /**
        This function is called when the DB connection is lost while executing SQL so that
        the current state can be freed while the driver-specific context data is still present
        reset the query but does not clear the SQL string or saved args.
     */
    DLLLOCAL void clear(ExceptionSink* xsink);

    //! Completely reset and clear.
    DLLLOCAL void reset(ExceptionSink* xsink);

    //! Returns the Qore character encoding for the connection
    DLLLOCAL const QoreEncoding* getQoreEncoding() const;

protected:
    //! ODBC statement handle.
    SQLHSTMT stmt = SQL_NULL_HSTMT;

    //! Copy of the original command.
    QoreString command;

    //! Arguments bound to the statement.
    ReferenceHolder<QoreListNode> bindArgs;

    //! Possible states when running getRowIntern() method.
    enum GetRowInternStatus {
        EGRIS_OK = 0,
        EGRIS_END,
        EGRIS_ERROR
    };

    //! Extract ODBC diagnostic and raise a Qore exception.
    /** @param err error "code"
        @param desc error description
        @param xsink exception sink
     */
    DLLLOCAL void handleStmtError(const char* err, const char* desc, ExceptionSink *xsink);

    //! Reset after lost connection.
    /** param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL virtual int resetAfterLostConnection(ExceptionSink* xsink);

    //! Execute a parsed statement with bound paramaters.
    /** @param str SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int execIntern(const char* str, SQLINTEGER textLen, ExceptionSink* xsink);

    //! Parse a Qore-style SQL statement.
    /** @param str Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int parse(QoreString* str, const QoreListNode* args, ExceptionSink* xsink);

    //! Get one row of the result set.
    /** @param status status of the function, 0 for OK, -1 for error, 1 after reaching the end of the result-set
        @param xsink exception sink

        @return one result-set row
     */
    DLLLOCAL QoreHashNode* getRowIntern(GetRowInternStatus& status, ExceptionSink* xsink);

    //! Return whether the passed arguments have arrays.
    DLLLOCAL bool hasArrays(const QoreListNode* args) const;

    //! Return size of arrays in the passed arguments.
    /** @param args SQL parameters

        @return parameter array size
     */
    DLLLOCAL size_t findArraySizeOfArgs(const QoreListNode* args) const;

    //! Return character count of the passed null-terminated UTF-8 string.
    /** @param str null-terminated UTF-8 string

        @return character count
     */
    SQLINTEGER getUTF8CharCount(char* str) {
        SQLINTEGER len = 0;
        for (; *str; ++str) if ((*str & 0xC0) != 0x80) ++len;
        return len;
    }

    //! Bind a simple list of SQL parameters.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindIntern(const QoreListNode* args, ExceptionSink* xsink);

    //! Bind a list of arrays of SQL parameters.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindInternArray(const QoreListNode* args, ExceptionSink* xsink);

private:
    //! Possible comment types. Used in the parse() method.
    enum SQLCommentType {
        ESCT_NONE = 0,
        ESCT_LINE,
        ESCT_BLOCK
    };

    //! ODBC connection wrapper.
    ODBCConnection* conn;

    //! Server timezone used for the date/time input and output parameters.
    const AbstractQoreZoneInfo* serverTz;

    //! Options regarding parameters and results.
    ODBCOptions options;

    //! Count of rows affected by the executed UPDATE, INSERT or DELETE statements.
    SQLLEN affectedRowCount = 0;

    //! Count of rows already read from the result-set.
    unsigned int readRows = 0;

    //! Count of required parameters for the SQL command.
    size_t paramCountInSql = 0;

    //! Temporary holder for params.
    intern::ParamHolder paramHolder;

    //! Temporary holder for parameter arrays.
    intern::ParamArrayHolder arrayHolder;

    //! Parameters which will be used in the statement.
    ReferenceHolder<QoreListNode> params;

    //! Result columns metadata.
    std::vector<ODBCResultColumn> resColumns;

    //! Free ODBC statement handle associated with this statement.
    void freeStatementHandle();

    //! Populate column hash.
    /** @param h column hash
        @param columns reference to a shortcut vector for column lists
     */
    DLLLOCAL void populateColumnHash(QoreHashNode& h, std::vector<QoreListNode*>& columns);

    //! Fetch metadata about result columns.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int fetchResultColumnMetadata(ExceptionSink* xsink);

    //! Get a column's value and return a Qore node made from it.
    /** @param column column number
        @param rcol result column metadata
        @param xsink exception sink

        @return result value wrapped in a Qore node, or 0 in case of error
     */
    DLLLOCAL QoreValue getColumnValue(int column, ODBCResultColumn& rcol, ExceptionSink* xsink);

    //! Bind a list of values as an array.
    /** @param column ODBC column number, starting from 1
        @param lst parameter list
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindParamArrayList(int column, const QoreListNode* lst, ExceptionSink* xsink);

    //! Bind a single value argument as an array.
    /** @param column ODBC column number, starting from 1
        @param arg single value parameter
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindParamArraySingleValue(int column, QoreValue arg, ExceptionSink* xsink);

    //! Bind a value or a list of values passed using \c odbc_bind as an array.
    /** @param column ODBC column number, starting from 1
        @param arg \c odbc_bind hash
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindParamArrayBindHash(int column, const QoreHashNode* arg, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_SLONG value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSLong(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_ULONG value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeULong(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_SSHORT value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSShort(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_USHORT value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeUShort(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_STINYINT value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSTinyint(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_UTINYINT value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeUTinyint(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int passed using \c odbc_bind as an ODBC \c SQL_C_FLOAT value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeFloat(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_TYPE_DATE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeDate(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_TYPE_TIME value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTime(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_TYPE_TIMESTAMP value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimestamp(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_YEAR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYear(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_MONTH value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMonth(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_YEAR_TO_MONTH value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearMonth(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_DAY value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDay(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_HOUR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHour(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_DAY_TO_HOUR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayHour(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_DAY_TO_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_DAY_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDaySecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_HOUR_TO_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_HOUR_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC \c SQL_C_INTERVAL_MINUTE_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_SLONG values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSLongArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_ULONG values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeULongArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_SSHORT values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSShortArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_USHORT values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeUShortArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_STINYINT values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeSTinyintArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_UTINYINT values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeUTinyintArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore int or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_FLOAT values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeFloatArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_TYPE_DATE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeDateArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_TYPE_TIME values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimeArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_TYPE_TIMESTAMP values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimestampArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_YEAR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_MONTH values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMonthArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_YEAR_TO_MONTH values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearMonthArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_DAY values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_HOUR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_DAY_TO_HOUR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayHourArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_DAY_TO_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_DAY_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDaySecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_HOUR_TO_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_HOUR_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC \c SQL_C_INTERVAL_MINUTE_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Create a new char array filled with string values from the passed Qore list.
    /** @param arg list of Qore strings used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromStringList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, size_t& maxlen, ExceptionSink* xsink);

    //! Create a new char array filled with stringified number values from the passed Qore list.
    /** @param arg list of Qore numbers used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromNumberList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, size_t& maxlen, ExceptionSink* xsink);

    //! Create a new void* array filled with values of binaries from the passed Qore list.
    /** @param arg list of Qore binaries used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the binary values will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromBinaryList(const QoreListNode* arg, void*& array, SQLLEN*& indArray, size_t& maxlen, ExceptionSink* xsink);

    //! Create an ODBC timestamp array filled in using Qore dates from the passed Qore list.
    /** @param arg list of Qore dates used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromAbsoluteDateList(const QoreListNode* arg, TIMESTAMP_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create an ODBC interval structure array filled in using Qore dates from the passed Qore list.
    /** @param arg list of Qore dates used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromRelativeDateList(const QoreListNode* arg, SQL_INTERVAL_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a bool array filled with Qore bools from the passed Qore list.
    /** @param arg list of Qore bools used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromBoolList(const QoreListNode* arg, int8_t*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create an int64 array filled with Qore ints from the passed Qore list.
    /** @param arg list of Qore ints used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromIntList(const QoreListNode* arg, int64*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a new char array filled with stringified int values from the passed Qore list.
    /** @param arg list of Qore ints used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createStrArrayFromIntList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, size_t& maxlen, ExceptionSink* xsink);

    //! Create a double array filled with Qore floats from the passed Qore list.
    /** @param arg list of Qore floats used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromFloatList(const QoreListNode* arg, double*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a new char array filled with the passed string value.
    /** @param arg string used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromString(const QoreStringNode* arg, char*& array, SQLLEN*& indArray, size_t& len, ExceptionSink* xsink);

    //! Create a new char array filled with the passed stringified number.
    /** @param arg number used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromNumber(const QoreNumberNode* arg, char*& array, SQLLEN*& indArray, size_t& len, ExceptionSink* xsink);

    //! Create a new void array filled with the passed binary's value.
    /** @param arg binary whose value will be used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param len size in bytes of the binary value
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromBinary(const BinaryNode* arg, void*& array, SQLLEN*& indArray, size_t& len, ExceptionSink* xsink);

    //! Create an array of ODBC timestamps initialized with the passed Qore date.
    /** @param arg date used for initializing the timestamps
        @param array pointer to the created array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromAbsoluteDate(const DateTimeNode* arg, TIMESTAMP_STRUCT*& array, ExceptionSink* xsink);

    //! Create an array of ODBC interval structures initialized with the passed Qore date.
    /** @param arg date used for initializing the interval structures
        @param array pointer to the created array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromRelativeDate(const DateTimeNode* arg, SQL_INTERVAL_STRUCT*& array, ExceptionSink* xsink);

    //! Create a bool array filled with the passed Qore bool value.
    /** @param arg Qore bool used for initializing the array values
        @param array pointer to the created array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromBool(bool arg, int8_t*& array, ExceptionSink* xsink);

    //! Create an int64 array filled with the passed Qore int value.
    /** @param arg Qore int used for initializing the array values
        @param array pointer to the created array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromInt(int64 arg, int64*& array, ExceptionSink* xsink);

    //! Create a new char array filled with the passed stringified int.
    /** @param arg int used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createStrArrayFromInt(QoreValue val, char*& array, SQLLEN*& indArray, size_t& len, ExceptionSink* xsink);

    //! Create a double array filled with the passed Qore float value.
    /** @param arg Qore float used for initializing the array values
        @param array pointer to the created array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromFloat(double val, double*& array, ExceptionSink* xsink);

    //! Create a new indicator array filled with the passed indicator value.
    /** @param indicator value used to fill the array
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL SQLLEN* createIndArray(SQLLEN indicator, ExceptionSink* xsink);

    //! Get a new C-style string in UTF-16 encoding from the passed Qore string.
    /** @param arg source Qore string
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the new string (caller owns it) or 0 in case of error
     */
    DLLLOCAL inline char* getCharsFromString(const QoreStringNode* arg, size_t& len, ExceptionSink* xsink);

    //! Get an ODBC timestamp from Qore date value.
    /** @param arg source Qore date

        @return ODBC timestamp structure
     */
    DLLLOCAL inline TIMESTAMP_STRUCT getTimestampFromDate(const DateTimeNode* arg);

    //! Get an ODBC interval structure from Qore date value.
    /** @param arg source Qore date

        @return ODBC interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getIntervalFromDate(const DateTimeNode* arg);

    //! Get an ODBC year interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC year interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getYearInterval(const DateTimeNode* arg);

    //! Get an ODBC month interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC month interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getMonthInterval(const DateTimeNode* arg);

    //! Get an ODBC year-to-month interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC year-to-month interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getYearMonthInterval(const DateTimeNode* arg);

    //! Get an ODBC day interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC day interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getDayInterval(const DateTimeNode* arg);

    //! Get an ODBC hour interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC hour interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getHourInterval(const DateTimeNode* arg);

    //! Get an ODBC minute interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC minute interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getMinuteInterval(const DateTimeNode* arg);

    //! Get an ODBC second interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC second interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getSecondInterval(const DateTimeNode* arg);

    //! Get an ODBC day-to-hour interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC day-to-hour interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getDayHourInterval(const DateTimeNode* arg);

    //! Get an ODBC day-to-minute interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC day-to-minute interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getDayMinuteInterval(const DateTimeNode* arg);

    //! Get an ODBC day-to-second interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC day-to-second interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getDaySecondInterval(const DateTimeNode* arg);

    //! Get an ODBC hour-to-minute interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC hour-to-minute interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getHourMinuteInterval(const DateTimeNode* arg);

    //! Get an ODBC hour-to-second interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC hour-to-second interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getHourSecondInterval(const DateTimeNode* arg);

    //! Get an ODBC minute-to-second interval from Qore date value.
    /** @param arg source Qore date

        @return ODBC minute-to-second interval structure
     */
    DLLLOCAL inline SQL_INTERVAL_STRUCT getMinuteSecondInterval(const DateTimeNode* arg);
};

class HashColumnAssignmentHelper : public HashAssignmentHelper {
public:
    DLLLOCAL HashColumnAssignmentHelper(QoreHashNode& h, const std::string& name) : HashAssignmentHelper(h, name.c_str()) {
        if (!**this)
            return;

        // Find a unique column name.
        unsigned num = 1;
        while (true) {
            QoreStringMaker tmp("%s_%d", name.c_str(), num);
            reassign(tmp.c_str());
            if (**this) {
                ++num;
                continue;
            }
            break;
        }
    }
};

char* ODBCStatement::getCharsFromString(const QoreStringNode* arg, size_t& len, ExceptionSink* xsink) {
    TempEncodingHelper tstr(arg, getQoreEncoding(), xsink);
    if (*xsink) {
        return nullptr;
    }
    len = tstr->size();
    /*
    // Remove BOM if present and encoding is UTF-16.
    const QoreEncoding* enc = getQoreEncoding();
    if ((enc == QCS_UTF16 || enc == QCS_UTF16LE || enc == QCS_UTF16BE) && len >= 2) {
        tstr.makeTemp();
        unsigned char* buf = reinterpret_cast<unsigned char*>(const_cast<char*>(tstr->c_str()));
        if ((buf[0] == 0xFF && buf[1] == 0xFE) || (buf[0] == 0xFE && buf[1] == 0xFF)) {
            len -= 2;
            memmove((void*)buf, (void*)(buf+2), len);
            buf[len] = '\0';
        } else if (len >= 3 && buf[0] == 0xEF && buf[1] == 0xBB && buf[2] == 0xBF) {
            len -= 3;
            memmove((void*)buf, (void*)(buf+3), len);
            buf[len] = '\0';
        }
    }
    */
    return tstr.giveBuffer();
}

TIMESTAMP_STRUCT ODBCStatement::getTimestampFromDate(const DateTimeNode* arg) {
    TIMESTAMP_STRUCT t;
    qore_tm info;
    info.clear();
    arg->getInfo(serverTz, info);
    t.year = info.year;
    t.month = info.month;
    t.day = info.day;
    t.hour = info.hour;
    t.minute = info.minute;
    t.second = info.second;
    if (options.frPrec >= 6) { // 6-9
        t.fraction = info.us * 1000;
    } else { // 1-5
        int n = std::pow(10, 6-options.frPrec);
        t.fraction = info.us / n;
        t.fraction *= n * 1000;
    }

    return t;
}

SQL_INTERVAL_STRUCT ODBCStatement::getIntervalFromDate(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT t;
    int64 secs = arg->getRelativeSeconds();
    int64 sign = secs >= 0 ? 1 : -1;
    secs *= sign;
    t.interval_type = SQL_IS_DAY_TO_SECOND;
    t.interval_sign = sign >= 0 ? SQL_FALSE : SQL_TRUE;
    t.intval.day_second.day = secs / 86400;
    secs -= t.intval.day_second.day * 86400;
    t.intval.day_second.hour = secs / 3600;
    secs -= t.intval.day_second.hour * 3600;
    t.intval.day_second.minute = secs / 60;
    secs -= t.intval.day_second.minute * 60;
    t.intval.day_second.second = secs;
    t.intval.day_second.fraction = 0;
    return t;
}

SQL_INTERVAL_STRUCT ODBCStatement::getYearInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.year_month.year = abs(arg->getYear());
    i.interval_type = SQL_IS_YEAR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMonthInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.year_month.month = abs(arg->getMonth());
    i.interval_type = SQL_IS_MONTH;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getYearMonthInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.year_month.year = abs(arg->getYear());
    i.intval.year_month.month = abs(arg->getMonth());
    i.interval_type = SQL_IS_YEAR_TO_MONTH;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.day = abs(arg->getDay());
    i.interval_type = SQL_IS_DAY;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.hour = abs(arg->getHour());
    i.interval_type = SQL_IS_HOUR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.minute = abs(arg->getMinute());
    i.interval_type = SQL_IS_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.second = abs(arg->getSecond());
    if (options.frPrec >= 6) { // 6-9
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) * 1000;
    }
    else { // 1-5
        int n = std::pow(10, 6-options.frPrec);
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) / n;
        i.intval.day_second.fraction *= n * 1000;
    }

    i.interval_type = SQL_IS_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayHourInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.day = abs(arg->getDay());
    i.intval.day_second.hour = abs(arg->getHour());
    i.interval_type = SQL_IS_DAY_TO_HOUR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.day = abs(arg->getDay());
    i.intval.day_second.hour = abs(arg->getHour());
    i.intval.day_second.minute = abs(arg->getMinute());
    i.interval_type = SQL_IS_DAY_TO_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDaySecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.day = abs(arg->getDay());
    i.intval.day_second.hour = abs(arg->getHour());
    i.intval.day_second.minute = abs(arg->getMinute());
    i.intval.day_second.second = abs(arg->getSecond());
    if (options.frPrec >= 6) { // 6-9
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) * 1000;
    }
    else { // 1-5
        int n = std::pow(10, 6-options.frPrec);
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) / n;
        i.intval.day_second.fraction *= n * 1000;
    }

    i.interval_type = SQL_IS_DAY_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.hour = abs(arg->getHour());
    i.intval.day_second.minute = abs(arg->getMinute());
    i.interval_type = SQL_IS_HOUR_TO_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.hour = abs(arg->getHour());
    i.intval.day_second.minute = abs(arg->getMinute());
    i.intval.day_second.second = abs(arg->getSecond());
    if (options.frPrec >= 6) { // 6-9
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) * 1000;
    }
    else { // 1-5
        int n = std::pow(10, 6-options.frPrec);
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) / n;
        i.intval.day_second.fraction *= n * 1000;
    }

    i.interval_type = SQL_IS_HOUR_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMinuteSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    i.intval.day_second.minute = abs(arg->getMinute());
    i.intval.day_second.second = abs(arg->getSecond());
    if (options.frPrec >= 6) { // 6-9
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) * 1000;
    }
    else { // 1-5
        int n = std::pow(10, 6-options.frPrec);
        i.intval.day_second.fraction = abs(arg->getMicrosecond()) / n;
        i.intval.day_second.fraction *= n * 1000;
    }

    i.interval_type = SQL_IS_MINUTE_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

} // namespace odbc

#endif // _QORE_MODULE_ODBC_ODBCSTATEMENT_H
