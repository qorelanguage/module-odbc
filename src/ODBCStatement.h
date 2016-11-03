/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCStatement.h

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

#ifndef _QORE_MODULE_ODBC_ODBCSTATEMENT_H
#define _QORE_MODULE_ODBC_ODBCSTATEMENT_H

#include <cerrno>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <sql.h>
#include <sqlext.h>

#include <qore/Qore.h>

#include "EnumNumericOption.h"
#include "ErrorHelper.h"
#include "ODBCResultColumn.h"
#include "ParamArrayHolder.h"
#include "ParamHolder.h"

class ODBCConnection;

//! A class representing one ODBC SQL statement.
class ODBCStatement {
public:
    //! Constructor.
    DLLLOCAL ODBCStatement(ODBCConnection* c, ExceptionSink* xsink);

    //! Constructor.
    DLLLOCAL ODBCStatement(Datasource* ds, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL ~ODBCStatement();

    //! Disabled copy constructor.
    DLLLOCAL ODBCStatement(const ODBCStatement& s) = delete;

    //! Disabled assignment operator.
    DLLLOCAL ODBCStatement& operator=(const ODBCStatement& s) = delete;

    //! Return how many rows were affected by the executed statement.
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

protected:
    //! ODBC statement handle.
    SQLHSTMT stmt;

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
    DLLLOCAL qore_size_t findArraySizeOfArgs(const QoreListNode* args) const;

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

    //! Server encoding used for SQL_CHAR input and output parameters.
    const QoreEncoding* serverEnc;

    //! Server timezone used for the date/time input and output parameters.
    const AbstractQoreZoneInfo* serverTz;

    //! Option used for deciding how NUMERIC results will be returned.
    NumericOption optNumeric;

    //! Count of rows affected by the executed UPDATE, INSERT or DELETE statements.
    SQLLEN affectedRowCount;

    //! Count of rows already read from the result-set.
    unsigned int readRows;

    //! Count of required parameters for the SQL command.
    qore_size_t paramCountInSql;

    //! Temporary holder for params.
    ParamHolder paramHolder;

    //! Temporary holder for parameter arrays.
    ParamArrayHolder arrayHolder;

    //! Parameters which will be used in the statement.
    ReferenceHolder<QoreListNode> params;

    //! Result columns metadata.
    std::vector<ODBCResultColumn> resColumns;

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
    DLLLOCAL inline AbstractQoreNode* getColumnValue(int column, ODBCResultColumn& rcol, ExceptionSink* xsink);

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
    DLLLOCAL int bindParamArraySingleValue(int column, const AbstractQoreNode* arg, ExceptionSink* xsink);

    //! Bind a value or a list of values passed using \c odbc_bind as an array.
    /** @param column ODBC column number, starting from 1
        @param arg \c odbc_bind hash
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindParamArrayBindHash(int column, const QoreHashNode* arg, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c DATE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeDate(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c TIME value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTime(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c TIMESTAMP value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimestamp(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_YEAR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYear(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_MONTH value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMonth(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_YEAR_TO_MONTH value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearMonth(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_DAY value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDay(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_HOUR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHour(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_DAY_TO_HOUR value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayHour(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_DAY_TO_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_DAY_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDaySecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_HOUR_TO_MINUTE value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_HOUR_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date passed using \c odbc_bind as an ODBC's \c INTERVAL_MINUTE_TO_SECOND value.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c DATE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeDateArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c TIME values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimeArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c TIMESTAMP values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeTimestampArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_YEAR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_MONTH values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMonthArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_YEAR_TO_MONTH values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntYearMonthArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_DAY values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_HOUR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_DAY_TO_HOUR values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayHourArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_DAY_TO_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDayMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_DAY_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntDaySecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_HOUR_TO_MINUTE values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_HOUR_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntHourSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Bind a Qore date or a list of them passed using \c odbc_bind as an array of ODBC's \c INTERVAL_MINUTE_TO_SECOND values.
    /** @param column ODBC column number, starting from 1
        @param arg value part of the \c odbc_bind hash
        @param ret reference to \c SQLRETURN where result of the binding will be stored
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bindTypeIntMinuteSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink);

    //! Create a new char array filled with string values from the passed Qore list.
    /** @param arg list of Qore strings used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromStringList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

    //! Create a new char array filled with stringified number values from the passed Qore list.
    /** @param arg list of Qore numbers used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromNumberList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

    //! Create a new void* array filled with values of binaries from the passed Qore list.
    /** @param arg list of Qore binaries used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param maxlen maximum length in bytes of the binary values will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromBinaryList(const QoreListNode* arg, void*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

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
    DLLLOCAL int createArrayFromBoolList(const QoreListNode* arg, bool*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create an int64 array filled with Qore ints from the passed Qore list.
    /** @param arg list of Qore ints used to fill the array
        @param array pointer to the created array will be written here (do not delete)
        @param indArray pointer to an accompanying indicator array will be written here (do not delete)
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int createArrayFromIntList(const QoreListNode* arg, int64*& array, SQLLEN*& indArray, ExceptionSink* xsink);

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
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL char* createArrayFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create a new char array filled with the passed stringified number.
    /** @param arg number used to fill the array
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL char* createArrayFromNumber(const QoreNumberNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create a new void array filled with the passed binary's value.
    /** @param arg binary whose value will be used to fill the array
        @param len size in bytes of the binary value
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL void* createArrayFromBinary(const BinaryNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create an array of ODBC timestamps initialized with the passed Qore date.
    /** @param arg date used for initializing the timestamps
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL TIMESTAMP_STRUCT* createArrayFromAbsoluteDate(const DateTimeNode* arg, ExceptionSink* xsink);

    //! Create an array of ODBC interval structures initialized with the passed Qore date.
    /** @param arg date used for initializing the interval structures
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL SQL_INTERVAL_STRUCT* createArrayFromRelativeDate(const DateTimeNode* arg, ExceptionSink* xsink);

    //! Create a bool array filled with the passed Qore bool value.
    /** @param arg Qore bool used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL bool* createArrayFromBool(const QoreBoolNode* arg, ExceptionSink* xsink);

    //! Create an int64 array filled with the passed Qore int value.
    /** @param arg Qore int used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL int64* createArrayFromInt(const QoreBigIntNode* arg, ExceptionSink* xsink);

    //! Create a double array filled with the passed Qore float value.
    /** @param arg Qore float used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array (do not delete) or 0 in case of error
     */
    DLLLOCAL double* createArrayFromFloat(const QoreFloatNode* arg, ExceptionSink* xsink);

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
    DLLLOCAL inline char* getCharsFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink);

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

inline AbstractQoreNode* ODBCStatement::getColumnValue(int column, ODBCResultColumn& rcol, ExceptionSink* xsink) {
    SQLLEN indicator;
    SQLRETURN ret;
    switch(rcol.dataType) {
        // Integer types.
        case SQL_INTEGER: {
            SQLINTEGER val;
            ret = SQLGetData(stmt, column, SQL_C_SLONG, &val, sizeof(SQLINTEGER), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreBigIntNode(val);
            }
            break;
        }
        case SQL_BIGINT: {
            SQLBIGINT val;
            ret = SQLGetData(stmt, column, SQL_C_SBIGINT, &val, sizeof(SQLBIGINT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreBigIntNode(val);
            }
            break;
        }
        case SQL_SMALLINT: {
            SQLSMALLINT val;
            ret = SQLGetData(stmt, column, SQL_C_SSHORT, &val, sizeof(SQLSMALLINT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreBigIntNode(val);
            }
            break;
        }
        case SQL_TINYINT: {
            SQLSCHAR val;
            ret = SQLGetData(stmt, column, SQL_C_STINYINT, &val, sizeof(SQLSCHAR), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreBigIntNode(val);
            }
            break;
        }

        // Float types.
        case SQL_FLOAT: {
            SQLFLOAT val;
            ret = SQLGetData(stmt, column, SQL_C_DOUBLE, &val, sizeof(SQLFLOAT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreFloatNode(val);
            }
            break;
        }
        case SQL_DOUBLE: {
            SQLDOUBLE val;
            ret = SQLGetData(stmt, column, SQL_C_DOUBLE, &val, sizeof(SQLDOUBLE), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreFloatNode(val);
            }
            break;
        }
        case SQL_REAL: {
            SQLREAL val;
            ret = SQLGetData(stmt, column, SQL_C_FLOAT, &val, sizeof(SQLREAL), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new QoreFloatNode(val);
            }
            break;
        }

        // Character types.
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR: {
            char unused[1];
            if (serverEnc) // Find out data size.
                ret = SQLGetData(stmt, column, SQL_C_CHAR, unused, 0, &indicator);
            else
                ret = SQLGetData(stmt, column, SQL_C_WCHAR, unused, 0, &indicator);

            if (ret == SQL_NO_DATA) // No data, therefore returning empty string.
                return new QoreStringNode;
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SQLLEN buflen = indicator + 2; // Ending \0 char.
                std::unique_ptr<char> buf(new (std::nothrow) char[buflen]);
                if (!buf.get()) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", readRows, column);
                    return 0;
                }
                if (serverEnc) {
                    ret = SQLGetData(stmt, column, SQL_C_CHAR, buf.get(), buflen, &indicator);
                    if (SQL_SUCCEEDED(ret)) {
                        SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, serverEnc));
                        return str.release();
                    }
                }
                else {
                    ret = SQLGetData(stmt, column, SQL_C_WCHAR, buf.get(), buflen, &indicator);
                    if (SQL_SUCCEEDED(ret)) {
#ifdef WORDS_BIGENDIAN
                        SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, QCS_UTF16BE));
#else
                        SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, QCS_UTF16LE));
#endif
                        return str.release();
                    }
                }
            }
            break;
        }
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR: {
            SQLWCHAR unused[1];
            ret = SQLGetData(stmt, column, SQL_C_WCHAR, unused, 0, &indicator); // Find out data size.
            if (ret == SQL_NO_DATA) // No data, therefore returning empty string.
                return new QoreStringNode;
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SQLLEN buflen = indicator + 2; // Ending \0 char.
                std::unique_ptr<char> buf(new (std::nothrow) char[buflen]);
                if (!buf.get()) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", readRows, column);
                    return 0;
                }
                ret = SQLGetData(stmt, column, SQL_C_WCHAR, reinterpret_cast<SQLWCHAR*>(buf.get()), buflen, &indicator);
                if (SQL_SUCCEEDED(ret)) {
#ifdef WORDS_BIGENDIAN
                    SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, QCS_UTF16BE));
#else
                    SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, QCS_UTF16LE));
#endif
                    return str.release();
                }
            }
            break;
        }

        // Binary types.
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY: {
            char unused[1];
            ret = SQLGetData(stmt, column, SQL_C_BINARY, unused, 0, &indicator); // Find out data size.
            if (ret == SQL_NO_DATA) // No data, therefore returning empty binary.
                return new BinaryNode;
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SQLLEN size = indicator;
                std::unique_ptr<char> buf(new (std::nothrow) char[size]);
                if (!buf.get()) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result binary data of row #%d, column #%d", readRows, column);
                    return 0;
                }
                ret = SQLGetData(stmt, column, SQL_C_BINARY, reinterpret_cast<void*>(buf.get()), size, &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    SimpleRefHolder<BinaryNode> bin(new BinaryNode(buf.release(), size));
                    return bin.release();
                }
            }
            break;
        }

        // Numeric/decimal types.
        case SQL_DECIMAL:
        case SQL_NUMERIC: {
            char val[128];
            ret = SQLGetData(stmt, column, SQL_C_CHAR, val, 128, &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                if (optNumeric == ENO_OPTIMAL) {
                    char* dot = strchr(val, '.');
                    if (!dot) {
                        errno = 0;
                        long long num = strtoll(val, 0, 10);
                        if (errno == ERANGE)
                            return new QoreNumberNode(val);
                        return new QoreBigIntNode(num);
                    }
                    SimpleRefHolder<QoreNumberNode> afterDot(new QoreNumberNode(dot+1));
                    if (afterDot->equals(0LL))
                        return new QoreBigIntNode(strtoll(val, 0, 10));
                    return new QoreNumberNode(val);
                }
                else if (optNumeric == ENO_STRING) {
                    return new QoreStringNode(val, QCS_UTF8);
                }
                else {
                    return new QoreNumberNode(val);
                }
            }
            break;
        }

        // One-bit value.
        case SQL_BIT: {
            SQLCHAR val;
            ret = SQLGetData(stmt, column, SQL_C_BIT, &val, sizeof(SQLCHAR), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return get_bool_node(val);
            }
            break;
        }

        // Time types.
        case SQL_TYPE_TIMESTAMP: {
            TIMESTAMP_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_TIMESTAMP, &val, sizeof(TIMESTAMP_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return DateTimeNode::makeAbsolute(serverTz, val.year, val.month, val.day, val.hour, val.minute, val.second, val.fraction/1000000);
            }
            break;
        }
        case SQL_TYPE_TIME: {
            TIME_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_TIME, &val, sizeof(TIME_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return DateTimeNode::makeRelative(0, 0, 0, val.hour, val.minute, val.second, 0);
            }
            break;
        }
        case SQL_TYPE_DATE: {
            DATE_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_DATE, &val, sizeof(DATE_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return DateTimeNode::makeAbsolute(serverTz, val.year, val.month, val.day, 0, 0, 0, 0);
            }
            break;
        }

        // Interval types.
        case SQL_INTERVAL_MONTH: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_MONTH, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_MONTH);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, val.intval.year_month.month, 0, 0, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_YEAR: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_YEAR, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_YEAR);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(val.intval.year_month.year, 0, 0, 0, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_YEAR_TO_MONTH: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_YEAR_TO_MONTH, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_YEAR_TO_MONTH);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(val.intval.year_month.year,
                    val.intval.year_month.month, 0, 0, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_DAY: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_DAY);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, val.intval.day_second.day, 0, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_HOUR: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_HOUR, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_HOUR);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, val.intval.day_second.hour, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_MINUTE: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_MINUTE, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_MINUTE);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, 0, val.intval.day_second.minute, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_SECOND: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, 0, 0, val.intval.day_second.second,
                    val.intval.day_second.fraction/1000000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_DAY_TO_HOUR: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY_TO_HOUR, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_DAY_TO_HOUR);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, val.intval.day_second.day,
                    val.intval.day_second.hour, 0, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_DAY_TO_MINUTE: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY_TO_MINUTE, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_DAY_TO_MINUTE);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, val.intval.day_second.day,
                    val.intval.day_second.hour, val.intval.day_second.minute, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_DAY_TO_SECOND: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_DAY_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, val.intval.day_second.day, val.intval.day_second.hour,
                    val.intval.day_second.minute, val.intval.day_second.second, val.intval.day_second.fraction/1000000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_HOUR_TO_MINUTE: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_HOUR_TO_MINUTE, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_HOUR_TO_MINUTE);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, val.intval.day_second.hour,
                    val.intval.day_second.minute, 0, 0, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_HOUR_TO_SECOND: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_HOUR_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_HOUR_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, val.intval.day_second.hour, val.intval.day_second.minute,
                    val.intval.day_second.second, val.intval.day_second.fraction/1000000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_MINUTE_TO_SECOND: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_MINUTE_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_MINUTE_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, 0, val.intval.day_second.minute,
                    val.intval.day_second.second, val.intval.day_second.fraction/1000000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_GUID: {
            SQLGUID val;
            ret = SQLGetData(stmt, column, SQL_C_GUID, &val, sizeof(SQLGUID), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SimpleRefHolder<QoreStringNode> s(new QoreStringNode);
                if (*s) {
                    s->sprintf("%d-%d-%d-%d", val.Data1, val.Data2, val.Data3, val.Data4[0]);
                    for (int i = 1; i < 8; i++)
                        s->sprintf(".%d", val.Data4[i]);
                }
                return s.release();
            }
            break;
        }
        default: {
            std::string s("do not know how to handle result value of type '%d'");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.c_str(), rcol.dataType);
            return 0;
        }
    }

    if (SQL_SUCCEEDED(ret) && indicator == SQL_NULL_DATA) {
        assert(rcol.nullable);
        return null();
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("error occured when getting value of row #%d, column #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.c_str(), readRows, column);
    }

    return 0;
}

char* ODBCStatement::getCharsFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    const QoreEncoding* enc = serverEnc;
    if (!enc) {
#ifdef WORDS_BIGENDIAN
        enc = const_cast<QoreEncoding*>(QCS_UTF16BE);
#else
        enc = const_cast<QoreEncoding*>(QCS_UTF16LE);
#endif
    }
    TempEncodingHelper tstr(arg, enc, xsink);
    len = tstr->size();
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
    t.fraction = info.us * 1000;
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
    int year = arg->getYear();
    i.intval.year_month.year = (year >= 0 ? year : -year);
    i.interval_type = SQL_IS_YEAR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMonthInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int month = arg->getMonth();
    i.intval.year_month.month = (month >= 0 ? month : -month);
    i.interval_type = SQL_IS_MONTH;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getYearMonthInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int year = arg->getYear();
    int month = arg->getMonth();
    i.intval.year_month.year = (year >= 0 ? year : -year);
    i.intval.year_month.month = (month >= 0 ? month : -month);
    i.interval_type = SQL_IS_YEAR_TO_MONTH;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int day = arg->getDay();
    i.intval.day_second.day = (day >= 0 ? day : -day);
    i.interval_type = SQL_IS_DAY;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int hour = arg->getHour();
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.interval_type = SQL_IS_HOUR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int minute = arg->getMinute();
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.interval_type = SQL_IS_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int second = arg->getSecond();
    i.intval.day_second.second = (second >= 0 ? second : -second);
    i.interval_type = SQL_IS_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayHourInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int day = arg->getDay();
    int hour = arg->getHour();
    i.intval.day_second.day = (day >= 0 ? day : -day);
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.interval_type = SQL_IS_DAY_TO_HOUR;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDayMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int day = arg->getDay();
    int hour = arg->getHour();
    int minute = arg->getMinute();
    i.intval.day_second.day = (day >= 0 ? day : -day);
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.interval_type = SQL_IS_DAY_TO_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getDaySecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int day = arg->getDay();
    int hour = arg->getHour();
    int minute = arg->getMinute();
    int second = arg->getSecond();
    int micros = arg->getMicrosecond();
    i.intval.day_second.day = (day >= 0 ? day : -day);
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.intval.day_second.second = (second >= 0 ? second : -second);
    i.intval.day_second.fraction = (micros >= 0 ? micros : -micros) * 1000;
    i.interval_type = SQL_IS_DAY_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourMinuteInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int hour = arg->getHour();
    int minute = arg->getMinute();
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.interval_type = SQL_IS_HOUR_TO_MINUTE;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getHourSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int hour = arg->getHour();
    int minute = arg->getMinute();
    int second = arg->getSecond();
    int micros = arg->getMicrosecond();
    i.intval.day_second.hour = (hour >= 0 ? hour : -hour);
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.intval.day_second.second = (second >= 0 ? second : -second);
    i.intval.day_second.fraction = (micros >= 0 ? micros : -micros) * 1000;
    i.interval_type = SQL_IS_HOUR_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

SQL_INTERVAL_STRUCT ODBCStatement::getMinuteSecondInterval(const DateTimeNode* arg) {
    SQL_INTERVAL_STRUCT i;
    memset(&i, 0, sizeof(SQL_INTERVAL_STRUCT));
    int minute = arg->getMinute();
    int second = arg->getSecond();
    int micros = arg->getMicrosecond();
    i.intval.day_second.minute = (minute >= 0 ? minute : -minute);
    i.intval.day_second.second = (second >= 0 ? second : -second);
    i.intval.day_second.fraction = (micros >= 0 ? micros : -micros) * 1000;
    i.interval_type = SQL_IS_MINUTE_TO_SECOND;
    i.interval_sign = arg->getRelativeSeconds() >= 0 ? SQL_FALSE : SQL_TRUE;
    return i;
}

#endif // _QORE_MODULE_ODBC_ODBCSTATEMENT_H
