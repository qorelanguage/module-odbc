/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCStatement.h

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

#ifndef _QORE_MODULE_ODBC_ODBCSTATEMENT_H
#define _QORE_MODULE_ODBC_ODBCSTATEMENT_H

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <sql.h>
#include <sqlext.h>

#include "qore/common.h"
#include "qore/QoreEncoding.h"
#include "qore/QoreString.h"
#include "qore/QoreStringNode.h"
#include "qore/AbstractQoreNode.h"
#include "qore/AbstractPrivateData.h"
#include "qore/BinaryNode.h"
#include "qore/QoreHashNode.h"
#include "qore/Datasource.h"
#include "qore/ExceptionSink.h"
#include "qore/DateTimeNode.h"
#include "qore/QoreBoolNode.h"
#include "qore/QoreBigIntNode.h"
#include "qore/QoreFloatNode.h"
#include "qore/QoreNumberNode.h"
#include "qore/QoreListNode.h"
#include "qore/QoreNullNode.h"

#include "ErrorHelper.h"
#include "ODBCResultColumn.h"
#include "ParamArrayHolder.h"
#include "ParamHolder.h"

class ODBCConnection;

//! A class representing one ODBC SQL statement.
class ODBCStatement {
private:
    //! Possible comment types. Used in the parse() method.
    enum SQLCommentType {
        ESCT_NONE = 0,
        ESCT_LINE,
        ESCT_BLOCK
    };

    //! Possible states when running getRowIntern() method.
    enum GetRowInternStatus {
        EGRIS_OK = 0,
        EGRIS_END,
        EGRIS_ERROR
    };

    //! ODBC connection wrapper.
    ODBCConnection* conn;

    //! ODBC statement handle.
    SQLHSTMT stmt;

    //! Temporary holder for params.
    ParamHolder paramHolder;

    //! Temporary holder for parameter arrays.
    ParamArrayHolder arrayHolder;

    //! Parameters which will be used in the statement.
    ReferenceHolder<QoreListNode> params;

    //! Result columns metadata.
    std::vector<ODBCResultColumn> resColumns;

    //! Extract ODBC diagnostic and raise a Qore exception.
    /** @param err error "code"
        @param desc error description
        @param xsink exception sink
     */
    void handleStmtError(const char* err, const char* desc, ExceptionSink *xsink);

    //! Fetch metadata about result columns.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int fetchResultColumnMetadata(ExceptionSink* xsink);

    //! Get one row of the result set.
    /** @param row row number from 0, used for error descriptions only
        @param status status of the function, 0 for OK, -1 for error, 1 after reaching the end of the result-set
        @param xsink exception sink

        @return one result-set row
     */
    QoreHashNode* getRowIntern(int row, GetRowInternStatus& status, ExceptionSink* xsink);

    //! Get a column's value and return a Qore node made from it.
    /** @param row row number from 0, used for error descriptions only
        @param column column number
        @param rcol result column metadata
        @param xsink exception sink

        @return result value wrapped in a Qore node
     */
    inline AbstractQoreNode* getColumnValue(int row, int column, ODBCResultColumn& rcol, ExceptionSink* xsink);

    //! Execute a parsed statement with bound paramaters.
    /** @param str SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int execIntern(const char* str, ExceptionSink* xsink);

    //! Parse a Qore-style SQL statement.
    /** @param str Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int parse(QoreString* str, const QoreListNode* args, ExceptionSink* xsink);

    //! Return whether the passed arguments have arrays.
    bool hasArrays(const QoreListNode* args) const;

    //! Return size of arrays in the passed arguments.
    /** @param args SQL parameters

        @return parameter array size
     */
    qore_size_t findArraySizeOfArgs(const QoreListNode* args) const;

    //! Bind a simple list of SQL parameters.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int bind(const QoreListNode* args, ExceptionSink* xsink);

    //! Bind a list of arrays of SQL parameters.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int bindArray(const QoreListNode* args, ExceptionSink* xsink);

    //! Bind a list of values as an array.
    /** @param column ODBC column number, starting from 1
        @param lst parameter list
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int bindParamArrayList(int column, const QoreListNode* lst, ExceptionSink* xsink);

    //! Bind a single value argument as an array.
    /** @param column ODBC column number, starting from 1
        @param arg single value parameter
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int bindParamArraySingleValue(int column, const AbstractQoreNode* arg, ExceptionSink* xsink);

    //! Create a new C-style string array filled with string values from the passed Qore list.
    /** @param arg list of Qore strings used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromStringList(const QoreListNode* arg, char**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

    //! Create a new C-style string array filled with stringified number values from the passed Qore list.
    /** @param arg list of Qore numbers used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param maxlen maximum length in bytes of the strings will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromNumberList(const QoreListNode* arg, char**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

    //! Create a new void* array filled with values of binaries from the passed Qore list.
    /** @param arg list of Qore binaries used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param maxlen maximum length in bytes of the binary values will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromBinaryList(const QoreListNode* arg, void**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink);

    //! Create an ODBC timestamp array filled in using Qore dates from the passed Qore list.
    /** @param arg list of Qore dates used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromAbsoluteDateList(const QoreListNode* arg, TIMESTAMP_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create an ODBC interval structure array filled in using Qore dates from the passed Qore list.
    /** @param arg list of Qore dates used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromRelativeDateList(const QoreListNode* arg, SQL_INTERVAL_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a bool array filled with Qore bools from the passed Qore list.
    /** @param arg list of Qore bools used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromBoolList(const QoreListNode* arg, bool*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create an int64 array filled with Qore ints from the passed Qore list.
    /** @param arg list of Qore ints used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromIntList(const QoreListNode* arg, int64*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a double array filled with Qore floats from the passed Qore list.
    /** @param arg list of Qore floats used to fill the array
        @param array pointer to the created array will be written here
        @param indArray pointer to an accompanying indicator array will be written here
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int createArrayFromFloatList(const QoreListNode* arg, double*& array, SQLLEN*& indArray, ExceptionSink* xsink);

    //! Create a new C-style string array and fill it with passed string value.
    /** @param arg string used to fill the array
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the created array
     */
    char** createArrayFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create a new C-style string array and fill it with passed stringified number.
    /** @param arg number used to fill the array
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the created array
     */
    char** createArrayFromNumber(const QoreNumberNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create a new void* array and fill it with passed binary's value.
    /** @param arg binary whose value will be used to fill the array
        @param len size in bytes of the binary value
        @param xsink exception sink

        @return pointer to the created array
     */
    void** createArrayFromBinary(const BinaryNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Create an array of ODBC timestamps initialized with the passed Qore date.
    /** @param arg date used for initializing the timestamps
        @param xsink exception sink

        @return pointer to the created array
     */
    TIMESTAMP_STRUCT* createArrayFromAbsoluteDate(const DateTimeNode* arg, ExceptionSink* xsink);

    //! Create an array of ODBC interval structures initialized with the passed Qore date.
    /** @param arg date used for initializing the interval structures
        @param xsink exception sink

        @return pointer to the created array
     */
    SQL_INTERVAL_STRUCT* createArrayFromRelativeDate(const DateTimeNode* arg, ExceptionSink* xsink);

    //! Create a bool array filled with the passed Qore bool value.
    /** @param arg Qore bool used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array
     */
    bool* createArrayFromBool(const QoreBoolNode* arg, ExceptionSink* xsink);

    //! Create an int64 array filled with the passed Qore int value.
    /** @param arg Qore int used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array
     */
    int64* createArrayFromInt(const QoreBigIntNode* arg, ExceptionSink* xsink);

    //! Create a double array filled with the passed Qore float value.
    /** @param arg Qore float used for initializing the array values
        @param xsink exception sink

        @return pointer to the created array
     */
    double* createArrayFromFloat(const QoreFloatNode* arg, ExceptionSink* xsink);

    //! Create a new indicator array filled with the passed indicator value.
    /** @param ind value used to fill the array
        @param xsink exception sink

        @return pointer to the created array
     */
    SQLLEN* createIndArray(SQLLEN ind, ExceptionSink* xsink);

    //! Get a new C-style string in UTF-16 encoding from the passed Qore string.
    /** @param arg source Qore string
        @param len size in bytes of the string will be written here
        @param xsink exception sink

        @return pointer to the new string
     */
    inline char* getCharsFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink);

    //! Get an ODBC timestamp from Qore date value.
    /** @param arg source Qore date

        @return ODBC timestamp structure
     */
    inline TIMESTAMP_STRUCT getTimestampFromDate(const DateTimeNode* arg);

    //! Get an ODBC interval structure from Qore date value.
    /** @param arg source Qore date

        @return ODBC interval structure
     */
    inline SQL_INTERVAL_STRUCT getIntervalFromDate(const DateTimeNode* arg);

    //! Disabled copy constructor.
    DLLLOCAL ODBCStatement(const ODBCStatement& s) : params(0, 0) {}

    //! Disabled assignment operator.
    DLLLOCAL ODBCStatement& operator=(const ODBCStatement& s) { return *this; }

public:
    //! Constructor.
    DLLLOCAL ODBCStatement(ODBCConnection* c, ExceptionSink* xsink);

    //! Constructor.
    DLLLOCAL ODBCStatement(Datasource* ds, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL ~ODBCStatement();

    //! Return how many rows were affected by the executed statement.
    DLLLOCAL int rowsAffected();

    //! Return if there are any results available.
    DLLLOCAL bool hasResultData();

    //! Get result hash.
    DLLLOCAL QoreHashNode* getOutputHash(ExceptionSink* xsink);

    //! Get result list.
    DLLLOCAL QoreListNode* getOutputList(ExceptionSink* xsink);

    //! Get one result row in the form of a hash.
    DLLLOCAL QoreHashNode* getSingleRow(ExceptionSink* xsink);

    //! Execute a Qore-style SQL statement with arguments.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute an SQL statement.
    /** @param cmd SQL statement
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int exec(const char* cmd, ExceptionSink* xsink);
};

inline AbstractQoreNode* ODBCStatement::getColumnValue(int row, int column, ODBCResultColumn& rcol, ExceptionSink* xsink) {
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
            ret = SQLGetData(stmt, column, SQL_C_CHAR, unused, 0, &indicator); // Find out data size.
            if (ret == SQL_NO_DATA) // No data, therefore returning empty string.
                return new QoreStringNode;
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SQLLEN buflen = indicator + 1; // Ending \0 char.
                std::unique_ptr<char> buf(new char[buflen]);
                if (buf.get() == NULL) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", row, column);
                    return 0;
                }
                ret = SQLGetData(stmt, column, SQL_C_CHAR, buf.get(), buflen, &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator, buflen, QCS_USASCII));
                    return str.release();
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
                std::unique_ptr<char> buf(new char[buflen]);
                if (buf.get() == NULL) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", row, column);
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
                std::unique_ptr<char> buf(new char[size]);
                if (buf.get() == NULL) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result binary data of row #%d, column #%d", row, column);
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

        // Various.
        case SQL_BIT: {
            SQLCHAR val;
            ret = SQLGetData(stmt, column, SQL_C_BIT, &val, sizeof(SQLCHAR), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return get_bool_node(val);
            }
            break;
        }
        /*case SQL_NUMERIC: {
            SQL_NUMERIC_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_NUMERIC, &val, sizeof(SQL_NUMERIC_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                // TODO
            }
            break;
        }
        case SQL_DECIMAL: {
            // TODO
            ret = SQLGetData(stmt, column, SQL_C_CHAR, buf, sizeof(buf), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                // TODO
            }
            break;
        }*/

        // Time types.
        case SQL_TYPE_TIMESTAMP: {
            TIMESTAMP_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_TIMESTAMP, &val, sizeof(TIMESTAMP_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return new DateTimeNode(val.year, val.month, val.day, val.hour, val.minute, val.second, val.fraction/1000000, false);
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
                return new DateTimeNode(val.year, val.month, val.day, 0, 0, 0, 0, false);
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
        /*case SQL_GUID: {
            // TODO
            ret = SQLGetData(stmt, column, SQL_C_CHAR, buf, sizeof(buf), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                // TODO
            }
            break;
        }*/
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
        xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.c_str(), row, column);
    }

    return 0;
}

char* ODBCStatement::getCharsFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink) {
#ifdef WORDS_BIGENDIAN
    TempEncodingHelper tstr(arg, QCS_UTF16BE, xsink);
#else
    TempEncodingHelper tstr(arg, QCS_UTF16LE, xsink);
#endif
    len = tstr->size();
    return tstr.giveBuffer();
}

TIMESTAMP_STRUCT ODBCStatement::getTimestampFromDate(const DateTimeNode* arg) {
    TIMESTAMP_STRUCT t;
    t.year = arg->getYear();
    t.month = arg->getMonth();
    t.day = arg->getDay();
    t.hour = arg->getHour();
    t.minute = arg->getMinute();
    t.second = arg->getSecond();
    t.fraction = arg->getMicrosecond() * 1000;
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

#endif // _QORE_MODULE_ODBC_ODBCSTATEMENT_H

