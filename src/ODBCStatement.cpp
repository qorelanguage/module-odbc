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

#include "qore/QoreLib.h"
#include "qore/DBI.h"

#include "ODBCConnection.h"


/////////////////////////////
//     Public methods     //
///////////////////////////

ODBCStatement::ODBCStatement(ODBCConnection* c, ExceptionSink* xsink) : conn(c), params(new QoreListNode, xsink) {
    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;

    // TODO
}

ODBCStatement::ODBCStatement(Datasource* ds, ExceptionSink* xsink) :
    conn(static_cast<ODBCConnection*>(ds->getPrivateData())),
    params(new QoreListNode, xsink)
{
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
    std::vector<std::string> cvec;
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
            xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.str().c_str(), row);
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
    GetRowInternStatus status;
    while (true) {
        ReferenceHolder<QoreHashNode> h(getRowIntern(row++, status, xsink), xsink);
        if (status == EGRIS_OK) { // Ok.
            l->push(h.release());
        }
        else if (status == EGRIS_END) { // End of result-set.
            break;
        }
        else { // status == EGRIS_ERROR
            return 0;
        }
    }

    return l.release();
}

QoreHashNode* ODBCStatement::getSingleRow(ExceptionSink* xsink) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    int row = 1;
    GetRowInternStatus status;
    ReferenceHolder<QoreHashNode> h(getRowIntern(row++, status, xsink), xsink);
    if (status == EGRIS_OK) { // Ok. Now have to check that there is only one row of data.
        ReferenceHolder<QoreHashNode> h2(getRowIntern(row, status, xsink), xsink);
        if (status == EGRIS_OK) {
            xsink->raiseException("DBI:ODBC:SELECT-ROW-ERROR", "SQL passed to selectRow() returned more than 1 row");
            return 0;
        }
        if (status == EGRIS_ERROR)
            return 0;
    }
    else if (status == EGRIS_END || status == EGRIS_ERROR) { // No data or error.
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

    if (bind(*params, xsink))
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
            xsink->raiseException("DBI:ODBC:COLUMN-METADATA-ERROR", s.str().c_str(), i+1);
            return -1;
        }

        nameLength += 8;
        col.name = new char[nameLength];

        ret = SQLColAttribute(stmt, i+1, SQL_DESC_NAME, &col.name, nameLength, NULL, NULL);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("error occured when fetching name of result column #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:COLUMN-METADATA-ERROR", s.str().c_str(), i+1);
            return -1;
        }
    }
    return 0;
}

QoreHashNode* ODBCStatement::getRowIntern(int row, GetRowInternStatus& status, ExceptionSink* xsink) {
    SQLRETURN ret = SQLFetch(stmt);
    if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
        status = EGRIS_END;
        return 0;
    }
    if (!SQL_SUCCEEDED(ret)) { // error
        std::stringstream s("error occured when fetching row #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.str().c_str(), row);
        status = EGRIS_ERROR;
        return 0;
    }

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink); // Row hash.
    if (*xsink) {
        status = EGRIS_ERROR;
        return 0;
    }

    int columns = resColumns.size();
    for (int i = 0; i < columns; i++) {
        ODBCResultColumn& col = resColumns[i];
        ReferenceHolder<AbstractQoreNode> n(xsink);
        n = getColumnValue(row, i+1, col, xsink);
        if (*xsink) {
            status = EGRIS_ERROR;
            return 0;
        }

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

    status = EGRIS_OK;
    return h.release();
}

int ODBCStatement::execIntern(const char* str, ExceptionSink* xsink) {
    SQLRETURN ret = SQLExecDirectA(stmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(str)), SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleStmtError("DBI:ODBC:EXEC-ERROR", "error during statement execution", xsink);
        return -1;
    }
    return 0;
}

int ODBCStatement::parse(QoreString* str, const QoreListNode* args, ExceptionSink* xsink) {
    char quote = 0;
    const char *p = str->getBuffer();
    QoreString tmp;
    int index = 0;
    SQLCommentType comment = ESCT_NONE;

    while (*p) {
        if (!quote) {
            if (comment == ESCT_NONE) {
                if ((*p) == '-' && (*(p+1)) == '-') {
                    comment = ESCT_LINE;
                    p += 2;
                    continue;
                }

                if ((*p) == '/' && (*(p+1)) == '*') {
                    comment = ESCT_BLOCK;
                    p += 2;
                    continue;
                }
            }
            else {
                if (comment == ESCT_LINE) {
                    if ((*p) == '\n' || ((*p) == '\r'))
                        comment = ESCT_NONE;
                    ++p;
                    continue;
                }

                assert(comment == ESCT_BLOCK);
                if ((*p) == '*' && (*(p+1)) == '/') {
                    comment = ESCT_NONE;
                    p += 2;
                    continue;
                }

                ++p;
                continue;
            }

            if ((*p) == '%' && (p == str->getBuffer() || !isalnum(*(p-1)))) { // Found value marker.
                int offset = p - str->getBuffer();

                p++;
                const AbstractQoreNode* v = args ? args->retrieve_entry(index++) : NULL;
                if ((*p) == 'd') {
                    DBI_concat_numeric(&tmp, v);
                    str->replace(offset, 2, &tmp);
                    p = str->getBuffer() + offset + tmp.strlen();
                    tmp.clear();
                    continue;
                }
                if ((*p) == 's') {
                    if (DBI_concat_string(&tmp, v, xsink))
                        return -1;
                    str->replace(offset, 2, &tmp);
                    p = str->getBuffer() + offset + tmp.strlen();
                    tmp.clear();
                    continue;
                }
                if ((*p) != 'v') {
                    xsink->raiseException("DBI:ODBC:PARSE-ERROR", "invalid value specification (expecting '%v' or '%%d', got %%%c)", *p);
                    return -1;
                }
                p++;
                if (isalpha(*p)) {
                    xsink->raiseException("DBI:ODBC:PARSE-ERROR", "invalid value specification (expecting '%v' or '%%d', got %%v%c*)", *p);
                    return -1;
                }

                str->replace(offset, 2, "?");
                p = str->getBuffer() + offset + 1;
                params->push(v->refSelf());
                continue;
            }

            // Allow escaping of '%' characters.
            if ((*p) == '\\' && (*(p+1) == ':' || *(p+1) == '%')) {
                str->splice(p - str->getBuffer(), 1, xsink);
                p += 2;
                continue;
            }
        }

        if (((*p) == '\'') || ((*p) == '\"')) {
            if (!quote)
                quote = *p;
            else if (quote == (*p))
                quote = 0;
            p++;
            continue;
        }

        p++;
    }
    return 0;
}

int ODBCStatement::bind(const QoreListNode* args, ExceptionSink* xsink) {
    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);
        SQLRETURN ret;

        if (!arg || is_null(arg) || is_nothing(arg)) { // Bind NULL argument.
            SQLLEN* len = tmp.addL(SQL_NULL_DATA);
            ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, NULL, 0, len);
            if (!SQL_SUCCEEDED(ret)) { // error
                std::stringstream s("failed binding NULL parameter with index %d (column %d)");
                ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                xsink->raiseException("DBI:ODBC:BIND-ERROR", s.str().c_str(), i, i+1);
                return -1;
            }
            continue;
        }

        qore_type_t ntype = arg ? arg->getType() : 0;
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(arg);
                TempEncodingHelper tstr(str, QEM.findCreate("UTF-16"), xsink);
                qore_size_t len = tstr->size();
                SQLLEN* indPtr = tmp.addL(len);
                char* cstr = tmp.addC(tstr.giveBuffer());
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                    len, 0, reinterpret_cast<SQLWCHAR*>(cstr), len, indPtr);
                break;
            }
            case NT_NUMBER: {
                QoreStringValueHelper vh(arg);
                qore_size_t len = vh->strlen();
                char* cstr = tmp.addC(vh.giveBuffer());
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, cstr, len, 0);
                break;
            }
            case NT_DATE: {
                const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
                if (date->isAbsolute()) {
                    TIMESTAMP_STRUCT t;
                    t.year = date->getYear();
                    t.month = date->getMonth();
                    t.day = date->getDay();
                    t.hour = date->getHour();
                    t.minute = date->getMinute();
                    t.second = date->getSecond();
                    t.fraction = date->getMicrosecond() * 1000;
                    TIMESTAMP_STRUCT* tval = tmp.addD(t);
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_TYPE_TIMESTAMP, 29, 9, tval, sizeof(TIMESTAMP_STRUCT), 0);
                }
                else {
                    int64 secs = date->getRelativeSeconds();
                    SQL_INTERVAL_STRUCT t;
                    t.interval_type = SQL_IS_DAY_TO_SECOND;
                    int64 sign = secs >= 0 ? 1 : -1;
                    secs *= sign;
                    t.interval_sign = sign >= 0 ? SQL_FALSE : SQL_TRUE;
                    t.intval.day_second.day = secs / 86400;
                    secs -= t.intval.day_second.day * 86400;
                    t.intval.day_second.hour = secs / 3600;
                    secs -= t.intval.day_second.hour * 3600;
                    t.intval.day_second.minute = secs / 60;
                    secs -= t.intval.day_second.minute * 60;
                    t.intval.day_second.second = secs;
                    t.intval.day_second.fraction = 0;
                    SQL_INTERVAL_STRUCT* tval = tmp.addT(t);
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_INTERVAL_DAY_TO_SECOND, 29, 9, tval, sizeof(SQL_INTERVAL_STRUCT), 0);
                }
                break;
            }
            case NT_INT: {
                const int64* ival = &(reinterpret_cast<const QoreBigIntNode*>(arg)->val);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                    SQL_BIGINT, 19, 0, const_cast<int64*>(ival), sizeof(int64), 0);
                break;
            }
            case NT_FLOAT: {
                const double* f = &(reinterpret_cast<const QoreFloatNode*>(arg)->f);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                    SQL_DOUBLE, 15, 0, const_cast<double*>(f), sizeof(double), 0);
                break;
            }
            case NT_BOOLEAN: {
                bool b = reinterpret_cast<const QoreBoolNode*>(arg)->getValue();
                bool* bval = tmp.addB(b);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BIT,
                    SQL_CHAR, 1, 0, bval, sizeof(bool), 0);
                break;
            }
            case NT_BINARY: {
                const BinaryNode* b = reinterpret_cast<const BinaryNode*>(arg);
                qore_size_t len = b->size();
                SQLLEN* indPtr = tmp.addL(len);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQL_BINARY, len, 0, const_cast<void*>(b->getPtr()), len, indPtr);
                break;
            }
            default: {
                std::stringstream s("do not know how to bind values of type '%s'");
                ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                xsink->raiseException("DBI:ODBC:BIND-ERROR", s.str().c_str(), arg->getTypeName());
                return -1;
            }
        } //  switch

        if (!SQL_SUCCEEDED(ret)) { // error
            std::stringstream s("failed binding parameter with index %d of type '%s'");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:BIND-ERROR", s.str().c_str(), i, arg->getTypeName());
            return -1;
        }
    }

    return 0;
}

