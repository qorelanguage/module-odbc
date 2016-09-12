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
}

ODBCStatement::ODBCStatement(Datasource* ds, ExceptionSink* xsink) :
    conn(static_cast<ODBCConnection*>(ds->getPrivateData())),
    params(new QoreListNode, xsink)
{
    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;
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
        SQLRETURN ret = SQLFetch(stmt);
        if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
            break;
        }
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching row #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.c_str(), row);
            return 0;
        }

        for (int j = 0; j < columnCount; j++) {
            ODBCResultColumn& rcol = resColumns[j];
            ReferenceHolder<AbstractQoreNode> n(getColumnValue(row, j+1, rcol, xsink), xsink);
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
    std::unique_ptr<QoreString> str(qstr->convertEncoding(QCS_USASCII, xsink));
    if (!str.get())
        return -1;

    if (parse(str.get(), args, xsink))
        return -1;

    if (hasArrays(args)) {
        if (bindArray(*params, xsink))
            return -1;
    }
    else {
        if (bind(*params, xsink))
            return -1;
    }

    return execIntern(str->getBuffer(), xsink);
}

int ODBCStatement::exec(const char* cmd, ExceptionSink* xsink) {
    return execIntern(cmd, xsink);
}


/////////////////////////////
//    Private methods     //
///////////////////////////

void ODBCStatement::handleStmtError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::string s(desc);
    ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
    xsink->raiseException(err, s.c_str());
}

int ODBCStatement::fetchResultColumnMetadata(ExceptionSink* xsink) {
    SQLSMALLINT columns;
    SQLRETURN ret = SQLNumResultCols(stmt, &columns);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleStmtError("DBI:ODBC:COLUMN-METADATA-ERROR", "error occured when fetching result column count", xsink);
        return -1;
    }

    char name[512];
    name[511] = '\0';
    resColumns.resize(columns);
    for (int i = 0; i < columns; i++) {
        ODBCResultColumn& col = resColumns[i];
        SQLSMALLINT nameLength;
        ret = SQLDescribeColA(stmt, i+1, reinterpret_cast<SQLCHAR*>(name), 512, &nameLength, &col.dataType,
            &col.colSize, &col.decimalDigits, &col.nullable);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching result column metadata of column #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:COLUMN-METADATA-ERROR", s.c_str(), i+1);
            return -1;
        }
        col.name = name;
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
        std::string s("error occured when fetching row #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.c_str(), row);
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

bool ODBCStatement::hasArrays(const QoreListNode* args) const {
    return findArraySizeOfArgs(args) > 0;
}

qore_size_t ODBCStatement::findArraySizeOfArgs(const QoreListNode* args) const {
    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);
        qore_type_t ntype = arg ? arg->getType() : 0;
        if (ntype == NT_LIST)
            return reinterpret_cast<const QoreListNode*>(arg)->size();
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
                std::string s("failed binding NULL parameter with index %d (column %d)");
                ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), i, i+1);
                return -1;
            }
            continue;
        }

        qore_type_t ntype = arg ? arg->getType() : 0;
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(arg);
                qore_size_t len;
                char* cstr = tmp.addC(getCharsFromString(str, len, xsink));
                if (*xsink)
                    return -1;
                SQLLEN* indPtr = tmp.addL(len);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                    len, 0, reinterpret_cast<SQLWCHAR*>(cstr), len, indPtr);
                break;
            }
            case NT_NUMBER: {
                QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
                if (*xsink)
                    return -1;
                qore_size_t len = vh->strlen();
                SQLLEN* indPtr = tmp.addL(len);
                char* cstr = tmp.addC(vh.giveBuffer());
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, cstr, len, indPtr);
                break;
            }
            case NT_DATE: {
                const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
                if (date->isAbsolute()) {
                    TIMESTAMP_STRUCT* tval = tmp.addD(getTimestampFromDate(date));
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_TYPE_TIMESTAMP, 29, 9, tval, sizeof(TIMESTAMP_STRUCT), 0);
                }
                else {
                    SQL_INTERVAL_STRUCT* tval = tmp.addT(getIntervalFromDate(date));
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
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
                xsink->raiseException("DBI:ODBC:BIND-ERROR", "do not know how to bind values of type '%s'", arg->getTypeName());
                return -1;
            }
        } // switch

        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("failed binding parameter with index %d of type '%s'");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), i, arg->getTypeName());
            return -1;
        }
    } // for

    return 0;
}

int ODBCStatement::bindArray(const QoreListNode* args, ExceptionSink* xsink) {
    qore_size_t arraySize = findArraySizeOfArgs(args);
    arrayHolder.setArraySize(arraySize);

    // Use column-wise binding.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

    // Set parameter array size.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(arraySize), 0);

    // Specify an array in which to return the status of each set of parameters.
    //SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, ParamStatusArray, 0); TODO

    // Specify an SQLUINTEGER value in which to return the number of sets of parameters processed.
    //SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &ParamsProcessed, 0);

    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);

        if (!arg || is_null(arg) || is_nothing(arg)) { // Handle NULL argument.
            handleParamArraySingleValue(i+1, arg, xsink);
            continue;
        }

        qore_type_t ntype = arg ? arg->getType() : 0;
        switch (ntype) {
            case NT_LIST:
                if (handleParamArrayList(i+1, reinterpret_cast<const QoreListNode*>(arg), xsink))
                    return -1;
                break;
            case NT_STRING:
            case NT_NUMBER:
            case NT_DATE:
            case NT_INT:
            case NT_FLOAT:
            case NT_BOOLEAN:
            case NT_BINARY:
                if (handleParamArraySingleValue(i+1, arg, xsink))
                    return -1;
                break;
            default:
                xsink->raiseException("DBI:ODBC:BIND-ERROR", "do not know how to bind values of type '%s'", arg->getTypeName());
                return -1;
        } // switch
    } // for

    return 0;
}

int ODBCStatement::handleParamArrayList(int column, const QoreListNode* lst, ExceptionSink* xsink) {
    qore_size_t count = lst->size();
    bool absoluteDate;
    SQLLEN* indArray;
    SQLRETURN ret;

    // Find out datatype of values in the list.
    qore_type_t ntype = NT_NULL;
    for (qore_size_t i = 0; i < count; i++) {
        const AbstractQoreNode* arg = lst->retrieve_entry(i);
        if (arg && !is_null(arg) && !is_nothing(arg)) {
            ntype = arg->getType();
            if (ntype == NT_DATE)
                absoluteDate = reinterpret_cast<const DateTimeNode*>(arg)->isAbsolute();
            break;
        }
    }

    switch (ntype) {
        case NT_STRING: {
            char** array;
            qore_size_t maxlen;
            if (createArrayFromStringList(lst, array, indArray, maxlen, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                maxlen, 0, reinterpret_cast<SQLWCHAR**>(array), maxlen, indArray);
            break;
        }
        case NT_NUMBER: {
            char** array;
            qore_size_t maxlen;
            if (createArrayFromNumberList(lst, array, indArray, maxlen, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR,
                SQL_CHAR, maxlen, 0, array, maxlen, indArray);
            break;
        }
        case NT_DATE: {
            if (absoluteDate) {
                TIMESTAMP_STRUCT* array;
                if (createArrayFromAbsoluteDateList(lst, array, indArray, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                    SQL_TYPE_TIMESTAMP, 29, 9, array, sizeof(TIMESTAMP_STRUCT), indArray);
            }
            else {
                SQL_INTERVAL_STRUCT* array;
                if (createArrayFromRelativeDateList(lst, array, indArray, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                    SQL_INTERVAL_DAY_TO_SECOND, 29, 9, array, sizeof(SQL_INTERVAL_STRUCT), indArray);
            }
            break;
        }
        case NT_INT: {
            int64* array;
            if (createArrayFromIntList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                SQL_BIGINT, 19, 0, array, sizeof(int64), indArray);
            break;
        }
        case NT_FLOAT: {
            double* array;
            if (createArrayFromFloatList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                SQL_DOUBLE, 15, 0, array, sizeof(double), 0);
            break;
        }
        case NT_BOOLEAN: {
            bool* array;
            if (createArrayFromBoolList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BIT,
                SQL_CHAR, 1, 0, array, sizeof(bool), 0);
            break;
        }
        case NT_BINARY: {
            void** array;
            qore_size_t maxlen;
            if (createArrayFromBinaryList(lst, array, indArray, maxlen, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BINARY,
                SQL_BINARY, maxlen, 0, array, maxlen, indArray);
            break;
        }
        case NT_NULL: {
            break;
        }
        default: {
            assert(false);
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "unknown parameter datatype; this error should never happen...");
            return -1;
        }
    } // switch

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array column #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), column);
        return -1;
    }

    return 0;
}

int ODBCStatement::handleParamArraySingleValue(int column, const AbstractQoreNode* arg, ExceptionSink* xsink) {
    qore_size_t arraySize = arrayHolder.getArraySize();
    SQLRETURN ret;

    if (!arg || is_null(arg) || is_nothing(arg)) { // Bind NULL argument.
        void** array = arrayHolder.getNullArray(xsink);
        if (!array)
            return -1;
        SQLLEN* indArray = arrayHolder.getNullIndArray(xsink);
        if (!indArray)
            return -1;

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 0, 0, array, 0, indArray);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("failed binding NULL single value parameter (column %d)");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), column);
            return -1;
        }
        return 0;
    }

    qore_type_t ntype = arg->getType();
    switch (ntype) {
        case NT_STRING: {
            qore_size_t len;
            char** array = createArrayFromString(reinterpret_cast<const QoreStringNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                len, 0, reinterpret_cast<SQLWCHAR**>(array), len, indArray);
            break;
        }
        case NT_NUMBER: {
            qore_size_t len;
            char** array = createArrayFromNumber(reinterpret_cast<const QoreNumberNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, array, len, indArray);
            break;
        }
        case NT_DATE: {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
            if (date->isAbsolute()) {
                TIMESTAMP_STRUCT* array = createArrayFromAbsoluteDate(date, xsink);
                if (*xsink || !array)
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_TYPE_TIMESTAMP, 29, 9, array, sizeof(TIMESTAMP_STRUCT), 0);
            }
            else {
                SQL_INTERVAL_STRUCT* array = createArrayFromRelativeDate(date, xsink);
                if (*xsink || !array)
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                        SQL_INTERVAL_DAY_TO_SECOND, 29, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
            }
            break;
        }
        case NT_INT: {
            int64* array = createArrayFromInt(reinterpret_cast<const QoreBigIntNode*>(arg), xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                SQL_BIGINT, 19, 0, array, sizeof(int64), 0);
            break;
        }
        case NT_FLOAT: {
            double* array = createArrayFromFloat(reinterpret_cast<const QoreFloatNode*>(arg), xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                SQL_DOUBLE, 15, 0, array, sizeof(double), 0);
            break;
        }
        case NT_BOOLEAN: {
            bool* array = createArrayFromBool(reinterpret_cast<const QoreBoolNode*>(arg), xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BIT,
                SQL_CHAR, 1, 0, array, sizeof(bool), 0);
            break;
        }
        case NT_BINARY: {
            qore_size_t len;
            void** array = createArrayFromBinary(reinterpret_cast<const BinaryNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQL_BINARY, len, 0, array, len, indArray);
            break;
        }
        default: {
            assert(false);
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "do not know how to bind values of type '%s'", arg->getTypeName());
            return -1;
        }
    } // switch

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array column #%d of type '%s'");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), column, arg->getTypeName());
        return -1;
    }

    return 0;
}

int ODBCStatement::createArrayFromStringList(const QoreListNode* arg, char**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
    array = arrayHolder.addCharArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const QoreStringNode* str = reinterpret_cast<const QoreStringNode*>(arg->retrieve_entry(i));
        if (!str) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }

        qore_size_t len = 0;
        array[i] = getCharsFromString(str, len, xsink);
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }
    return 0;
}

int ODBCStatement::createArrayFromNumberList(const QoreListNode* arg, char**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
    array = arrayHolder.addCharArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const QoreNumberNode* n = reinterpret_cast<const QoreNumberNode*>(arg->retrieve_entry(i));
        if (!n) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }

        QoreStringValueHelper vh(n, QCS_USASCII, xsink);
        if (*xsink)
            return -1;
        qore_size_t len = vh->strlen();
        array[i] = vh.giveBuffer();
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }
    return 0;
}

int ODBCStatement::createArrayFromBinaryList(const QoreListNode* arg, void**& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
    array = arrayHolder.addVoidArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const BinaryNode* bin = reinterpret_cast<const BinaryNode*>(arg->retrieve_entry(i));
        if (!bin || bin->getPtr() == 0) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = const_cast<void*>(bin->getPtr());
        indArray[i] = bin->size();
        maxlen = (maxlen >= indArray[i]) ? maxlen : indArray[i];
    }
    return 0;
}

int ODBCStatement::createArrayFromAbsoluteDateList(const QoreListNode* arg, TIMESTAMP_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addTimestampArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg->retrieve_entry(i));
        if (!date) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = getTimestampFromDate(date);
        indArray[i] = sizeof(TIMESTAMP_STRUCT);
    }
    return 0;
}

int ODBCStatement::createArrayFromRelativeDateList(const QoreListNode* arg, SQL_INTERVAL_STRUCT*& array, SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg->retrieve_entry(i));
        if (!date) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = getIntervalFromDate(date);
        indArray[i] = sizeof(SQL_INTERVAL_STRUCT);
    }
    return 0;
}

int ODBCStatement::createArrayFromBoolList(const QoreListNode* arg, bool*& array, SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addBoolArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const QoreBoolNode* bn = reinterpret_cast<const QoreBoolNode*>(arg->retrieve_entry(i));
        if (!bn) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = bn->getValue();
        indArray[i] = sizeof(bool);
    }
    return 0;
}

int ODBCStatement::createArrayFromIntList(const QoreListNode* arg, int64*& array, SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addIntArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const QoreBigIntNode* in = reinterpret_cast<const QoreBigIntNode*>(arg->retrieve_entry(i));
        if (!in) {
            array[i] = 0;
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = in->val;
        indArray[i] = sizeof(int64);
    }
    return 0;
}

int ODBCStatement::createArrayFromFloatList(const QoreListNode* arg, double*& array, SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addFloatArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++) {
        const QoreFloatNode* fn = reinterpret_cast<const QoreFloatNode*>(arg->retrieve_entry(i));
        if (!fn) {
            array[i] = 0;
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = fn->f;
        indArray[i] = sizeof(double);
    }
    return 0;
}

char** ODBCStatement::createArrayFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    char* val = tmp.addC(getCharsFromString(arg, len, xsink));
    char** array = arrayHolder.addSingleValCharArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

char** ODBCStatement::createArrayFromNumber(const QoreNumberNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
    if (*xsink)
        return 0;
    len = vh->strlen();
    char* val = tmp.addC(vh.giveBuffer());
    char** array = arrayHolder.addSingleValCharArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

void** ODBCStatement::createArrayFromBinary(const BinaryNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    len = arg->size();
    void* val = const_cast<void*>(arg->getPtr());
    void** array = arrayHolder.addVoidArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

TIMESTAMP_STRUCT* ODBCStatement::createArrayFromAbsoluteDate(const DateTimeNode* arg, ExceptionSink* xsink) {
    assert(arg->isAbsolute());
    TIMESTAMP_STRUCT val = getTimestampFromDate(arg);
    TIMESTAMP_STRUCT* array = arrayHolder.addTimestampArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

SQL_INTERVAL_STRUCT* ODBCStatement::createArrayFromRelativeDate(const DateTimeNode* arg, ExceptionSink* xsink) {
    assert(arg->isRelative());
    SQL_INTERVAL_STRUCT val = getIntervalFromDate(arg);
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

bool* ODBCStatement::createArrayFromBool(const QoreBoolNode* arg, ExceptionSink* xsink) {
    bool val = arg->getValue();
    bool* array = arrayHolder.addBoolArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

int64* ODBCStatement::createArrayFromInt(const QoreBigIntNode* arg, ExceptionSink* xsink) {
    int64 val = arg->val;
    int64* array = arrayHolder.addIntArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

double* ODBCStatement::createArrayFromFloat(const QoreFloatNode* arg, ExceptionSink* xsink) {
    double val = arg->f;
    double* array = arrayHolder.addFloatArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return array;
}

SQLLEN* ODBCStatement::createIndArray(SQLLEN ind, ExceptionSink* xsink) {
    SQLLEN* array = arrayHolder.addIndArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = ind;
    return array;
}

