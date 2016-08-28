/* -*- indent-tabs-mode: nil -*- */
/*
  ODBCStatement.cpp

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

#include "ODBCStatement.h"

#include <memory>

#include "ODBCConnection.h"


/////////////////////////////
//     Public methods     //
///////////////////////////

ODBCStatement::ODBCStatement(ODBCConnection* c, ExceptionSink* xsink) : conn(c) {
    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;

    // TODO
}

ODBCStatement::ODBCStatement(Datasource* ds, ExceptionSink* xsink) : conn(static_cast<ODBCConnection*>(ds->getPrivateData())) {
    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;

    // TODO
}

ODBCStatement::~ODBCStatement() {
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

int ODBCStatement::rowsAffected() {
    SQLLEN len = -1;
    SQLRETURN ret = SQLRowCount(stmt, &len);
    if (!SQL_SUCCEEDED(ret) || len == -1) { // error
        return -1;
    }
    return len;
}

bool ODBCStatement::hasResultData() {
    SQLSMALLINT columns;
    SQLNumResultCols(stmt, &columns);
    return columns != 0;
}

QoreHashNode* ODBCStatement::getOutputHash(ExceptionSink* xsink) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink);
    if (*xsink)
        return 0;

    int columnCount = resColumns.size();
    std::vector<QoreListNode*> columns;
    columns.resize(columnCount);

    // Assign unique column names.
    strvec_t cvec;
    cvec.reserve(columnCount);
    for (int i = 0; i < columnCount; i++) {
        ODBCResultColumn& col = resColumns[i];

        HashAssignmentHelper hah(**h, col.name);
        if (*hah) { // Find a unique column name.
            unsigned num = 1;
            while (true) {
                QoreStringMaker tmp("%s_%d", col.name, num);
                hah.reassign(tmp.c_str());
                if (*hah) {
                    ++num;
                    continue;
                }
                cvec.push_back(tmp.c_str());
                break;
            }
        }
        else {
            cvec.push_back(col.name);
        }
        columns[i] = new QoreListNode;
        hah.assign(columns[i], xsink);
    }

    int row = 0;
    while (true) {
        // TODO - fetch row
        SQLRETURN ret = SQLFetch(stmt);
        if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
            break;
        }
        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("error occured when fetching row #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            ErrorHelper::exception(xsink, "DBI:ODBC:FETCH-ERROR", s.str().c_str(), row);
            status = -1; // Error.
            return 0;
        }

        for (int j = 0; j < columnCount; j++) {
            ODBCResultColumn& rcol = resColumns[j];
            ReferenceHolder<AbstractQoreNode> n(getColumnValue(row, j, rcol, xsink), xsink);
            if (!n || *xsink)
                return 0;

            (columns[j])->push(n.release());
        }
    }

    return h.release();
}

QoreListNode* ODBCStatement::getOutputList(ExceptionSink* xsink) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreListNode> l(new QoreListNode(), xsink);
    if (*xsink)
        return 0;

    int row = 1;
    int status;
    while (true) {
        ReferenceHolder<QoreHashNode> h(getRowIntern(row++, status, xsink), xsink);
        if (status == 0) {
            l->push(h.release());
        }
        else if (status == 1) {
            break;
        }
        else { // status == -1
            return 0;
        }
    }

    return l.release();
}

QoreHashNode* ODBCStatement::getSingleRow(ExceptionSink* xsink) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    int row = 1;
    int status;
    ReferenceHolder<QoreHashNode> h(getRowIntern(row++, status, xsink), xsink);
    if (status == 0) { // Ok. Now have to check that there is only one row of data.
        ReferenceHolder<QoreHashNode> h2(getRowIntern(row, status, xsink), xsink);
        if (status == 0) {
            xsink->raiseException("DBI:ODBC:SELECT-ROW-ERROR", "SQL passed to selectRow() returned more than 1 row");
            return 0;
        }
        if (status == -1)
            return 0;
    }
    else if (status == 1 || status == -1) { // No data or error.
        return 0;
    }

    return h.release();
}

int ODBCStatement::exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    // Convert string to required character encoding or copy.
    std::unique_ptr<QoreString> str(qstr->convertEncoding(QEM.findCreate("ASCII"), xsink));
    if (!str.get())
        return -1;

    if (parse(str.get(), args, xsink))
        return -1;

    if (bind(args, xsink))
        return -1;

    return execIntern(str->getBuffer(), xsink);
}

int ODBCStatement::exec(const char* cmd, ExceptionSink* xsink) {
    return execIntern(cmd, xsink);
}


/////////////////////////////
//    Private methods     //
///////////////////////////

void ODBCStatement::handleStmtError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::stringstream s(desc);
    ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
    xsink->raiseException(err, s.str().c_str());
}

int ODBCStatement::fetchResultColumnMetadata(ExceptionSink* xsink) {
    SQLSMALLINT columns;
    SQLNumResultCols(stmt, &columns);

    resColumns.resize(columns);

    for (int i = 0; i < columns; i++) {
        ODBCResultColumn& col = resColumns[i];
        SQLSMALLINT nameLength;
        SQLRETURN ret = SQLDescribeCol(stmt, i+1, NULL, 0, &nameLength, &col.dataType,
            &col.colSize, &col.decimalDigits, &col.nullable);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("error occured when fetching result column metadata of column #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            ErrorHelper::exception(xsink, "DBI:ODBC:COLUMN-METADATA-ERROR", s.str().c_str(), i+1);
            return -1;
        }

        nameLength += 8;
        col.name = new char[nameLength];

        ret = SQLColAttribute(stmt, i+1, SQL_DESC_NAME, &col.name, nameLength, NULL, NULL);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("error occured when fetching name of result column #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            ErrorHelper::exception(xsink, "DBI:ODBC:COLUMN-METADATA-ERROR", s.str().c_str(), i+1);
            return -1;
        }
    }
    return 0;
}

QoreHashNode* ODBCStatement::getRowIntern(int row, int& status, ExceptionSink* xsink) {
    SQLRETURN ret = SQLFetch(stmt);
    if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
        status = 1;
        return 0;
    }
    if (!SQL_SUCCEEDED(ret)) { // error
        std::stringstream s("error occured when fetching row #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        ErrorHelper::exception(xsink, "DBI:ODBC:FETCH-ERROR", s.str().c_str(), row);
        status = -1; // Error.
        return 0;
    }

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink); // Row hash.
    if (*xsink)
        return 0;

    int columns = resColumns.size();
    for (int i = 0; i < columns; i++) {
        ODBCResultColumn& col = resColumns[i];
        ReferenceHolder<AbstractQoreNode> n(xsink);
        /*switch(col.dataType) {
            case : {
                break;
            }
            default: {
                std::stringstream s("do not know how to handle result value of type '%d'");
                ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                ErrorHelper::exception(xsink, "DBI:ODBC:RESULT-ERROR", s.str(), col.dataType);
                status = -1; // Error.
                return 0;
            }
        }*/
        n = getColumnValue(row, i+1, col, xsink);

        HashAssignmentHelper hah(**h, col.name);
        if (*hah) { // Find a unique column name.
            unsigned num = 1;
            while (true) {
                QoreStringMaker tmp("%s_%d", col.name, num);
                hah.reassign(tmp.c_str());
                if (*hah) {
                    ++num;
                    continue;
                }
                break;
            }
        }

        hah.assign(n.release(), xsink);
    }

    status = 0; // Everything ok.
    return h.release();
}

int ODBCStatement::execIntern(const char* str, Exceptionsink* xsink) {
    SQLRETURN ret = SQLExecDirect(stmt, str, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleStmtError("DBI:ODBC:EXEC-ERROR", "error during statement execution", xsink);
        return -1;
    }
    return 0;
}

int ODBCStatement::parse(const QoreString* str, const QoreListNode* args, ExceptionSink* xsink) {
    // TODO
    return 0;
}

int ODBCStatement::bind(const QoreListNode* args, ExceptionSink* xsink) {
    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);

        if (!arg || is_null(arg) || is_nothing(arg)) { // Bind NULL argument.
            // TODO
        }

        qore_type_t ntype = arg ? arg->getType() : 0;
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(arg);
                TempEncodingHelper tstr(str, QEM.findCreate("UTF-16"), xsink);
                qore_size_t len = tstr.size();
                char* cstr = tmp.addC(tstr.giveBuffer());
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_WCHAR,
                    SQLWCHAR, len, 0, reinterpret_cast<wchar_t*>(cstr), len, 0);
                break;
            }
            case NT_NUMBER: {
                QoreStringValueHelper vh(arg);
                qore_size_t len = vh->strlen();
                char* cstr = tmp.addC(vh->giveBuffer());
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR,
                    SQLCHAR, len, 0, cstr, len, 0);
                break;
            }
            case NT_DATE: {
                const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
                if (date->isAbsolute) {
                    TIMESTAMP_STRUCT t;
                    t.year = date->getYear();
                    t.month = date->getMonth();
                    t.day = date->getDay();
                    t.hour = date->getHour();
                    t.minute = date->getMinute();
                    t.second = date->getSecond();
                    t.fraction = date->getMicrosecond() * 1000;
                    TIMESTAMP_STRUCT* tval = tmp.addD(t);
                    SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_TIMESTAMP_STRUCT, 29, 9, tval, sizeof(TIMESTAMP_STRUCT), 0);
                }
                else {
                    int64 secs = date->getRelativeSeconds();
                    SQL_INTERVAL_STRUCT t;
                    t.interval_type = SQL_IS_DAY_TO_SECOND;
                    int64 sign = secs >= 0 ? 1 : -1;
                    secs *= sign;
                    t.interval_sign = sign >= 0 ? SQL_FALSE : SQL_TRUE;
                    t.day_second.day = secs / 86400;
                    secs -= t.day_second.day * 86400;
                    t.day_second.hour = secs / 3600;
                    secs -= t.day_second.hour * 3600;
                    t.day_second.minute = secs / 60;
                    secs -= t.day_second.minute * 60;
                    t.day_second.second = secs;
                    t.day_second.fraction = 0;
                    SQL_INTERVAL_STRUCT* tval = tmp.addT(t);
                    SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_INTERVAL_STRUCT, 29, 9, tval, sizeof(SQL_INTERVAL_STRUCT), 0);
                }
                break;
            }
            case NT_INT: {
                int64* ival = &(reinterpret_cast<const QoreBigIntNode*>(arg)->val);
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                    SQLBIGINT, 19, 0, ival, sizeof(int64), 0);
                break;
            }
            case NT_FLOAT: {
                double* f = &(reinterpret_cast<const QoreFloatNode*>(arg)->f);
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                    SQLDOUBLE, 15, 0, f, sizeof(double), 0);
                break;
            }
            case NT_BOOLEAN: {
                bool b = &(reinterpret_cast<const QoreBoolNode*>(arg)->getValue());
                bool* bval = tmp.addB(b);
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BIT,
                    SQLCHAR, 1, 0, bval, sizeof(bool), 0);
                break;
            }
            case NT_BINARY: {
                const BinaryNode* b = reinterpret_cast<const BinaryNode*>(arg);
                qore_size_t len = b->size();
                SQLRETURN ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQLCHAR, len, 0, b->getPtr(), len, 0);
                break;
            }
            default: {
                std::stringstream s("do not know how to bind values of type '%s'");
                ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                ErrorHelper::exception(xsink, "DBI:ODBC:BIND-ERROR", s.str().c_str(), arg->getTypeName());
                return -1;
            }
        } //  switch

        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("failed binding parameter with index %d of type '%s'");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            ErrorHelper::exception(xsink, "DBI:ODBC:BIND-ERROR", s.str().c_str(), i, arg->getTypeName());
            return -1;
        }
    }

    return 0;
}

