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

#ifndef _QORE_ODBCSTATEMENT_H
#define _QORE_ODBCSTATEMENT_H

#include <cstring>
#include <memory>
#include <sstream>
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
#include "qore/QoreListNode.h"
#include "qore/QoreNullNode.h"

#include "ErrorHelper.h"
#include "ODBCResultColumn.h"
#include "TempParamHolder.h"

class ODBCConnection;

//! A class representing one ODBC SQL statement.
class ODBCStatement {
private:
    enum SQLCommentType {
        ESCT_NONE = 0,
        ESCT_LINE,
        ESCT_BLOCK
    };

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
    TempParamHolder tmp;

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

    //! Bind a simple list of SQL parameters.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    int bind(const QoreListNode* args, ExceptionSink* xsink);

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
                std::unique_ptr<char> buf(new char[indicator]);
                if (buf.get() == NULL) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", row, column);
                    return 0;
                }
                ret = SQLGetData(stmt, column, SQL_C_CHAR, buf.get(), indicator, &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator-1, indicator, QEM.findCreate("ASCII")));
                    return str.release();
                }
            }
            break;

            /*SimpleRefHolder<QoreStringNode> val(new QoreStringNode);
            while (true) {
                ret = SQLGetData(stmt, column, SQL_C_CHAR, buf, COLUMN_VALUE_BUF_SIZE, &indicator);
                if (!SQL_SUCCEEDED(ret) || (indicator == SQL_NULL_DATA))
                    break;
                val->concat(buf);
                if (ret == SQL_SUCCESS_WITH_INFO) {
                    char state[7];
                    ErrorHelper::extractState(SQL_HANDLE_STMT, stmt, state);
                    if (strcmp(state, "01004") == 0 || indicator > 0) {
                        continue;
                    }
                    else {
                        assert(false);
                    }
                }
                else {
                    return val->release();
                }
            }
            break;*/
        }
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR: {
            SQLWCHAR unused[1];
            ret = SQLGetData(stmt, column, SQL_C_WCHAR, unused, 0, &indicator); // Find out data size.
            if (ret == SQL_NO_DATA) // No data, therefore returning empty string.
                return new QoreStringNode;
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                std::unique_ptr<char> buf(new char[indicator]);
                if (buf.get() == NULL) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, column #%d", row, column);
                    return 0;
                }
                ret = SQLGetData(stmt, column, SQL_C_WCHAR, reinterpret_cast<SQLWCHAR*>(buf.get()), indicator, &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    SimpleRefHolder<QoreStringNode> str(new QoreStringNode(buf.release(), indicator-1, indicator, QEM.findCreate("UTF-16")));
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
            std::stringstream s("do not know how to handle result value of type '%d'");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.str().c_str(), rcol.dataType);
            return 0;
        }
    }

    if (SQL_SUCCEEDED(ret) && indicator == SQL_NULL_DATA) {
        assert(rcol.nullable);
        return null();
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        std::stringstream s("error occured when getting value of row #%d, column #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.str().c_str(), row, column);
    }

    return 0;
}

#endif // _QORE_ODBCSTATEMENT_H

