/* -*- indent-tabs-mode: nil -*- */
/*
  ODBCStatement.cpp

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

#include "ODBCStatement.h"

#include "qore/QoreLib.h"
#include "qore/DBI.h"

#include "ODBCColumnSizeConstants.h"
#include "ODBCConnection.h"


/////////////////////////////
//     Public methods     //
///////////////////////////

ODBCStatement::ODBCStatement(ODBCConnection* c, ExceptionSink* xsink) :
    conn(c),
    serverEnc(0),
    serverTz(conn->getServerTimezone()),
    optNumeric(conn->getNumericOption()),
    affectedRowCount(0),
    readRows(0),
    paramCountInSql(0),
    params(new QoreListNode, xsink)
{
    const char* dbEnc = c->getDatasource()->getDBEncoding();
    if (dbEnc)
        serverEnc = QEM.findCreate(dbEnc);

    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;
}

ODBCStatement::ODBCStatement(Datasource* ds, ExceptionSink* xsink) :
    conn(static_cast<ODBCConnection*>(ds->getPrivateData())),
    serverEnc(0),
    serverTz(conn->getServerTimezone()),
    optNumeric(conn->getNumericOption()),
    affectedRowCount(0),
    readRows(0),
    paramCountInSql(0),
    params(new QoreListNode, xsink)
{
    const char* dbEnc = ds->getDBEncoding();
    if (dbEnc)
        serverEnc = QEM.findCreate(dbEnc);

    conn->allocStatementHandle(stmt, xsink);
    if (*xsink)
        return;
}

ODBCStatement::~ODBCStatement() {
    SQLCloseCursor(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

bool ODBCStatement::hasResultData() {
    SQLSMALLINT columns;
    SQLNumResultCols(stmt, &columns);
    return columns != 0;
}

QoreHashNode* ODBCStatement::describe(ExceptionSink* xsink) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink);

    QoreString namestr("name");
    QoreString maxsizestr("maxsize");
    QoreString typestr("type");
    QoreString dbtypestr("native_type");
    QoreString internalstr("internal_id");
    int columnCount = resColumns.size();

    // Assign unique column names.
    for (int i = 0; i < columnCount; i++) {
        ODBCResultColumn& col = resColumns[i];

        HashColumnAssignmentHelper hah(**h, col.name);

        ReferenceHolder<QoreHashNode> desc(new QoreHashNode, xsink);
        desc->setKeyValue(namestr, new QoreStringNode(col.name), xsink);
        desc->setKeyValue(internalstr, new QoreBigIntNode(col.dataType), xsink);
        desc->setKeyValue(maxsizestr, new QoreBigIntNode(col.byteSize), xsink);

        switch (col.dataType) {
            // Integer types.
            case SQL_INTEGER:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_INT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTEGER"), xsink);
                break;
            case SQL_BIGINT:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_INT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BIGINT"), xsink);
                break;
            case SQL_SMALLINT:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_INT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_SMALLINT"), xsink);
                break;
            case SQL_TINYINT:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_INT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TINYINT"), xsink);
                break;

            // Float types.
            case SQL_FLOAT:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_FLOAT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_FLOAT"), xsink);
                break;
            case SQL_DOUBLE:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_FLOAT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_DOUBLE"), xsink);
                break;
            case SQL_REAL:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_FLOAT), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_REAL"), xsink);
                break;

            // Character types.
            case SQL_CHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_CHAR"), xsink);
                break;
            case SQL_VARCHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_VARCHAR"), xsink);
                break;
            case SQL_LONGVARCHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_LONGVARCHAR"), xsink);
                break;
            case SQL_WCHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WCHAR"), xsink);
                break;
            case SQL_WVARCHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WVARCHAR"), xsink);
                break;
            case SQL_WLONGVARCHAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WLONGVARCHAR"), xsink);
                break;

            // Binary types.
            case SQL_BINARY:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_BINARY), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BINARY"), xsink);
                break;
            case SQL_VARBINARY:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_BINARY), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_VARBINARY"), xsink);
                break;
            case SQL_LONGVARBINARY:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_BINARY), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_LONGVARBINARY"), xsink);
                break;

            // Various.
            case SQL_BIT:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_BOOLEAN), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BIT"), xsink);
                break;
            case SQL_NUMERIC:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_NUMBER), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_NUMERIC"), xsink);
                break;
            case SQL_DECIMAL:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_NUMBER), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_DECIMAL"), xsink);
                break;

            // Time types.
            case SQL_TYPE_TIMESTAMP:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_TIMESTAMP"), xsink);
                break;
            case SQL_TYPE_TIME:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_TIME"), xsink);
                break;
            case SQL_TYPE_DATE:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_DATE"), xsink);
                break;

            // Interval types.
            case SQL_INTERVAL_MONTH:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MONTH"), xsink);
                break;
            case SQL_INTERVAL_YEAR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_YEAR"), xsink);
                break;
            case SQL_INTERVAL_YEAR_TO_MONTH:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_YEAR_TO_MONTH"), xsink);
                break;
            case SQL_INTERVAL_DAY:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY"), xsink);
                break;
            case SQL_INTERVAL_HOUR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR"), xsink);
                break;
            case SQL_INTERVAL_MINUTE:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_SECOND:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_SECOND"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_HOUR:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_HOUR"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_MINUTE:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_SECOND:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_SECOND"), xsink);
                break;
            case SQL_INTERVAL_HOUR_TO_MINUTE:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR_TO_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_HOUR_TO_SECOND:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR_TO_SECOND"), xsink);
                break;
            case SQL_INTERVAL_MINUTE_TO_SECOND:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_DATE), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MINUTE_TO_SECOND"), xsink);
                break;
            case SQL_GUID:
                desc->setKeyValue(typestr, new QoreBigIntNode(NT_STRING), xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_GUID"), xsink);
                break;
        } // switch

        hah.assign(desc.release(), xsink);
    }

    return h.release();
}

void ODBCStatement::populateColumnHash(QoreHashNode& h, std::vector<QoreListNode*>& columns) {
    int columnCount = resColumns.size();
    columns.resize(columnCount);

    // Assign unique column names.
    for (int i = 0; i < columnCount; i++) {
        ODBCResultColumn& col = resColumns[i];

        HashColumnAssignmentHelper hah(h, col.name);
        columns[i] = new QoreListNode;
        hah.assign(columns[i], 0);
    }
}

QoreHashNode* ODBCStatement::getOutputHash(ExceptionSink* xsink, bool emptyHashIfNothing, int maxRows) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink);
    std::vector<QoreListNode*> columns;

    int rowCount = 0;
    while (true) {
        SQLRETURN ret = SQLFetch(stmt);
        if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
            break;
        }
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching row #%d");
            ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.c_str(), readRows);
            return 0;
        }

        for (int j = 0; j < resColumns.size(); j++) {
            ODBCResultColumn& rcol = resColumns[j];
            ReferenceHolder<AbstractQoreNode> n(getColumnValue(j+1, rcol, xsink), xsink);
            if (!n || *xsink)
                return 0;

            if (h->empty())
               populateColumnHash(**h, columns);

            (columns[j])->push(n.release());
        }
        readRows++;
        rowCount++;
        if (rowCount == maxRows && maxRows > 0)
            break;
    }

    if (!rowCount && !emptyHashIfNothing)
       populateColumnHash(**h, columns);

    return h.release();
}

QoreListNode* ODBCStatement::getOutputList(ExceptionSink* xsink, int maxRows) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreListNode> l(new QoreListNode(), xsink);

    int rowCount = 0;
    GetRowInternStatus status;
    while (true) {
        ReferenceHolder<QoreHashNode> h(getRowIntern(status, xsink), xsink);
        if (status == EGRIS_OK) { // Ok.
            l->push(h.release());
            rowCount++;
            if (rowCount == maxRows && maxRows > 0)
                break;
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

    GetRowInternStatus status;
    ReferenceHolder<QoreHashNode> h(getRowIntern(status, xsink), xsink);
    if (status == EGRIS_OK) { // Ok. Now have to check that there is only one row of data.
        ReferenceHolder<QoreHashNode> h2(getRowIntern(status, xsink), xsink);
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
    // Convert string to required character encoding.
    std::unique_ptr<QoreString> str(qstr->convertEncoding(QCS_UTF8, xsink));
    if (!str.get())
        return -1;

    if (parse(str.get(), args, xsink))
        return -1;

    if (hasArrays(args)) {
        if (bindInternArray(*params, xsink))
            return -1;
    }
    else {
        if (bindIntern(*params, xsink))
            return -1;
    }

#ifdef WORDS_BIGENDIAN
    TempEncodingHelper tstr(str.get(), QCS_UTF16BE, xsink);
#else
    TempEncodingHelper tstr(str.get(), QCS_UTF16LE, xsink);
#endif
    if (*xsink)
        return -1;

    SQLINTEGER textLen = getUTF8CharCount(const_cast<char*>(str->c_str()));
    return execIntern(tstr->getBuffer(), textLen, xsink);
}

int ODBCStatement::exec(const QoreString* qstr, ExceptionSink* xsink) {
    // Convert string to required character encoding.
    std::unique_ptr<QoreString> str(qstr->convertEncoding(QCS_UTF8, xsink));
    if (!str.get())
        return -1;

#ifdef WORDS_BIGENDIAN
    TempEncodingHelper tstr(str.get(), QCS_UTF16BE, xsink);
#else
    TempEncodingHelper tstr(str.get(), QCS_UTF16LE, xsink);
#endif
    if (*xsink)
        return -1;

    SQLINTEGER textLen = getUTF8CharCount(const_cast<char*>(str->c_str()));
    return execIntern(tstr->getBuffer(), textLen, xsink);
}


/////////////////////////////
//   Protected methods    //
///////////////////////////


void ODBCStatement::handleStmtError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::string s(desc);
    ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
    xsink->raiseException(err, s.c_str());
}

int ODBCStatement::execIntern(const char* str, SQLINTEGER textLen, ExceptionSink* xsink) {
    SQLRETURN ret;
    if (str)
        ret = SQLExecDirectW(stmt, reinterpret_cast<SQLWCHAR*>(const_cast<char*>(str)), textLen);
    else
        ret = SQLExecute(stmt);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleStmtError("DBI:ODBC:EXEC-ERROR", "error during statement execution", xsink);
        affectedRowCount = -1;
        return -1;
    }

    // Get count of affected rows.
    SQLLEN len = -1;
    ret = SQLRowCount(stmt, &len);
    if (!SQL_SUCCEEDED(ret) || len == -1) { // error
        affectedRowCount = -1;
        return -1;
    }
    affectedRowCount = len;

    return 0;
}

int ODBCStatement::parse(QoreString* str, const QoreListNode* args, ExceptionSink* xsink) {
    char quote = 0;
    const char *p = str->getBuffer();
    QoreString tmp;
    int index = 0;
    SQLCommentType comment = ESCT_NONE;
    params = new QoreListNode;
    paramCountInSql = 0;

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
                const AbstractQoreNode* v = args ? args->retrieve_entry(index++) : 0;
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
                paramCountInSql++;
                if (v)
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

QoreHashNode* ODBCStatement::getRowIntern(GetRowInternStatus& status, ExceptionSink* xsink) {
    SQLRETURN ret = SQLFetch(stmt);
    if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
        status = EGRIS_END;
        return 0;
    }
    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("error occured when fetching row #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:FETCH-ERROR", s.c_str(), readRows);
        status = EGRIS_ERROR;
        return 0;
    }

    ReferenceHolder<QoreHashNode> h(new QoreHashNode, xsink); // Row hash.

    int columns = resColumns.size();
    for (int i = 0; i < columns; i++) {
        ODBCResultColumn& col = resColumns[i];
        ReferenceHolder<AbstractQoreNode> n(xsink);
        n = getColumnValue(i+1, col, xsink);
        if (*xsink) {
            status = EGRIS_ERROR;
            return 0;
        }

        HashAssignmentHelper hah(**h, col.name);
        if (*hah) { // Find a unique column name.
            unsigned num = 1;
            while (true) {
                QoreStringMaker tmp("%s_%d", col.name.c_str(), num);
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
    readRows++;

    status = EGRIS_OK;
    return h.release();
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

int ODBCStatement::bindIntern(const QoreListNode* args, ExceptionSink* xsink) {
    // Clear previous parameters.
    arrayHolder.clear();
    paramHolder.clear();

    // Check that enough parameters were passed for binding.
    if (args && paramCountInSql > args->size()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "not enough parameters passed for binding; %u required but only %u passed",
            paramCountInSql, args->size());
        return -1;
    }

    // Set parameter array size to 1.
    qore_size_t one = 1;
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(one), 0);

    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);
        SQLRETURN ret;

        if (!arg || is_null(arg) || is_nothing(arg)) { // Bind NULL argument.
            SQLLEN* len = paramHolder.addLength(SQL_NULL_DATA);
            ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, 0, 0, len);
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
                char* cstr = paramHolder.addChars(getCharsFromString(str, len, xsink));
                if (*xsink)
                    return -1;
                SQLLEN* indPtr = paramHolder.addLength(len);
                if (serverEnc) {
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                        len, 0, reinterpret_cast<SQLCHAR*>(cstr), len, indPtr);
                }
                else  {
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                        len, 0, reinterpret_cast<SQLWCHAR*>(cstr), len, indPtr);
                }
                break;
            }
            case NT_NUMBER: {
                QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
                if (*xsink)
                    return -1;
                qore_size_t len = vh->strlen();
                SQLLEN* indPtr = paramHolder.addLength(len);
                char* cstr = paramHolder.addChars(vh.giveBuffer());
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, cstr, len, indPtr);
                break;
            }
            case NT_DATE: {
                const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
                if (date->isAbsolute()) {
                    TIMESTAMP_STRUCT* tval = paramHolder.addTimestamp(getTimestampFromDate(date));
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                        SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, tval, sizeof(TIMESTAMP_STRUCT), 0);
                }
                else {
                    SQL_INTERVAL_STRUCT* tval = paramHolder.addInterval(getIntervalFromDate(date));
                    ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                        SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, tval, sizeof(SQL_INTERVAL_STRUCT), 0);
                }
                break;
            }
            case NT_INT: {
                const int64* ival = &(reinterpret_cast<const QoreBigIntNode*>(arg)->val);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                    SQL_BIGINT, BIGINT_COLSIZE, 0, const_cast<int64*>(ival), sizeof(int64), 0);
                break;
            }
            case NT_FLOAT: {
                const double* f = &(reinterpret_cast<const QoreFloatNode*>(arg)->f);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                    SQL_DOUBLE, DOUBLE_COLSIZE, 0, const_cast<double*>(f), sizeof(double), 0);
                break;
            }
            case NT_BOOLEAN: {
                bool b = reinterpret_cast<const QoreBoolNode*>(arg)->getValue();
                bool* bval = paramHolder.addBool(b);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BIT,
                    SQL_CHAR, 1, 0, bval, sizeof(bool), 0);
                break;
            }
            case NT_BINARY: {
                const BinaryNode* b = reinterpret_cast<const BinaryNode*>(arg);
                qore_size_t len = b->size();
                SQLLEN* indPtr = paramHolder.addLength(len);
                ret = SQLBindParameter(stmt, i+1, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQL_BINARY, len, 0, const_cast<void*>(b->getPtr()), len, indPtr);
                break;
            }
            case NT_HASH: {
                const QoreHashNode* h = reinterpret_cast<const QoreHashNode*>(arg);
                const AbstractQoreNode* odbct = h->getKeyValue("^odbct^");
                const AbstractQoreNode* value = h->getKeyValue("^value^");
                if (!odbct || !value || odbct->getType() != NT_INT) {
                    xsink->raiseException("DBI:ODBC:BIND-ERROR", "hash parameter not in correct format for odbc_bind; ODBC module cannot bind hash values");
                    return -1;
                }
                const QoreBigIntNode* odbcType = reinterpret_cast<const QoreBigIntNode*>(odbct);
                switch (odbcType->val) {
                    case SQL_TYPE_DATE:
                        if (bindTypeDate(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_TYPE_TIME:
                        if (bindTypeTime(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_TYPE_TIMESTAMP:
                        if (bindTypeTimestamp(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_MONTH:
                        if (bindTypeIntMonth(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_YEAR:
                        if (bindTypeIntYear(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_YEAR_TO_MONTH:
                        if (bindTypeIntMonth(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_DAY:
                        if (bindTypeIntDay(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_HOUR:
                        if (bindTypeIntHour(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_MINUTE:
                        if (bindTypeIntMinute(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_SECOND:
                        if (bindTypeIntSecond(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_DAY_TO_HOUR:
                        if (bindTypeIntDayHour(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_DAY_TO_MINUTE:
                        if (bindTypeIntDayMinute(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_DAY_TO_SECOND:
                        if (bindTypeIntDaySecond(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_HOUR_TO_MINUTE:
                        if (bindTypeIntHourMinute(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_HOUR_TO_SECOND:
                        if (bindTypeIntHourSecond(i+1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_INTERVAL_MINUTE_TO_SECOND:
                        if (bindTypeIntMinuteSecond(i+1, value, ret, xsink))
                            return -1;
                        break;
                    default:
                        xsink->raiseException("DBI:ODBC:BIND-ERROR", "odbc_bind used with an unknown type identifier: %d", odbcType->val);
                        return -1;
                }
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

int ODBCStatement::bindInternArray(const QoreListNode* args, ExceptionSink* xsink) {
    // Clear previous parameters.
    arrayHolder.clear();
    paramHolder.clear();

    // Check that enough parameters were passed for binding.
    if (args && paramCountInSql != args->size()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR",
            "mismatch between the parameter list size and number of parameters in the SQL command; %u required, %u passed",
            paramCountInSql, args->size());
        return -1;
    }

    // Find parameter array size.
    qore_size_t arraySize = findArraySizeOfArgs(args);
    arrayHolder.setArraySize(arraySize);

    // Use column-wise binding.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

    // Set parameter array size.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(arraySize), 0);

    // Specify an array in which to return the status of each set of parameters.
    // Since this is unnecessarily precise status reporting, we set it to 0,
    // so that these states are not generated.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, 0, 0);

    // Specify an SQLUINTEGER value in which to return the number of sets of parameters processed.
    // Also unnecessary information, therefore set to 0.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, 0, 0);

    qore_size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        const AbstractQoreNode* arg = args->retrieve_entry(i);

        if (!arg || is_null(arg) || is_nothing(arg)) { // Handle NULL argument.
            bindParamArraySingleValue(i+1, arg, xsink);
            continue;
        }

        qore_type_t ntype = arg ? arg->getType() : 0;
        switch (ntype) {
            case NT_LIST:
                if (bindParamArrayList(i+1, reinterpret_cast<const QoreListNode*>(arg), xsink))
                    return -1;
                break;
            case NT_STRING:
            case NT_NUMBER:
            case NT_DATE:
            case NT_INT:
            case NT_FLOAT:
            case NT_BOOLEAN:
            case NT_BINARY:
                if (bindParamArraySingleValue(i+1, arg, xsink))
                    return -1;
                break;
            case NT_HASH: {
                const QoreHashNode* h = reinterpret_cast<const QoreHashNode*>(arg);
                const AbstractQoreNode* odbct = h->getKeyValue("^odbct^");
                const AbstractQoreNode* value = h->getKeyValue("^value^");
                if (!odbct || !value || odbct->getType() != NT_INT) {
                    xsink->raiseException("DBI:ODBC:BIND-ERROR", "hash parameter not in correct format for odbc_bind; ODBC module cannot bind hash values");
                    return -1;
                }
                if (bindParamArrayBindHash(i+1, h, xsink))
                    return -1;
                break;
            }
            default:
                xsink->raiseException("DBI:ODBC:BIND-ERROR", "do not know how to bind values of type '%s'", arg->getTypeName());
                return -1;
        } // switch
    } // for

    return 0;
}


/////////////////////////////
//    Private methods     //
///////////////////////////

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

        ret = SQLColAttributeA(stmt, i+1, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &col.byteSize);
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

int ODBCStatement::bindParamArrayList(int column, const QoreListNode* lst, ExceptionSink* xsink) {
    qore_size_t count = lst->size();
    bool absoluteDate;
    SQLLEN* indArray;
    SQLRETURN ret;

    // Find out datatype of values in the list.
    qore_type_t ntype = NT_NULL;
    for (qore_size_t i = 0; i < count; i++) {
        const AbstractQoreNode* arg = lst->retrieve_entry(i);
        if (arg && !is_null(arg) && !is_nothing(arg)) {
            if (ntype == NT_NULL) {
                ntype = arg->getType();
                if (ntype == NT_DATE)
                    absoluteDate = reinterpret_cast<const DateTimeNode*>(arg)->isAbsolute();
            }
            else if (ntype != arg->getType()) { // Different types in the same array -> error.
                xsink->raiseException("DBI:ODBC:BIND-ERROR", "different datatypes in the same parameter array for column #%d", column);
                return -1;
            }
        }
    }

    switch (ntype) {
        case NT_STRING: {
            char* array;
            qore_size_t maxlen;
            if (createArrayFromStringList(lst, array, indArray, maxlen, xsink))
                return -1;
            if (serverEnc) {
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                    maxlen, 0, reinterpret_cast<SQLCHAR*>(array), maxlen, indArray);
            }
            else  {
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                    maxlen, 0, reinterpret_cast<SQLWCHAR*>(array), maxlen, indArray);
            }
            break;
        }
        case NT_NUMBER: {
            char* array;
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
                    SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, array, sizeof(TIMESTAMP_STRUCT), indArray);
            }
            else {
                SQL_INTERVAL_STRUCT* array;
                if (createArrayFromRelativeDateList(lst, array, indArray, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                    SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), indArray);
            }
            break;
        }
        case NT_INT: {
            int64* array;
            if (createArrayFromIntList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                SQL_BIGINT, BIGINT_COLSIZE, 0, array, sizeof(int64), indArray);
            break;
        }
        case NT_FLOAT: {
            double* array;
            if (createArrayFromFloatList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                SQL_DOUBLE, DOUBLE_COLSIZE, 0, array, sizeof(double), 0);
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
            void* array;
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

int ODBCStatement::bindParamArraySingleValue(int column, const AbstractQoreNode* arg, ExceptionSink* xsink) {
    SQLRETURN ret;

    if (!arg || is_null(arg) || is_nothing(arg)) { // Bind NULL argument.
        char* array = arrayHolder.getNullArray(xsink);
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
            char* array = createArrayFromString(reinterpret_cast<const QoreStringNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !indArray)
                return -1;
            if (serverEnc) {
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                    len, 0, reinterpret_cast<SQLCHAR*>(array), len, indArray);
            }
            else  {
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR,
                    len, 0, reinterpret_cast<SQLWCHAR*>(array), len, indArray);
            }
            break;
        }
        case NT_NUMBER: {
            qore_size_t len;
            char* array = createArrayFromNumber(reinterpret_cast<const QoreNumberNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !indArray)
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
                        SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, array, sizeof(TIMESTAMP_STRUCT), 0);
            }
            else {
                SQL_INTERVAL_STRUCT* array = createArrayFromRelativeDate(date, xsink);
                if (*xsink || !array)
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                        SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
            }
            break;
        }
        case NT_INT: {
            int64* array = createArrayFromInt(reinterpret_cast<const QoreBigIntNode*>(arg), xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                SQL_BIGINT, BIGINT_COLSIZE, 0, array, sizeof(int64), 0);
            break;
        }
        case NT_FLOAT: {
            double* array = createArrayFromFloat(reinterpret_cast<const QoreFloatNode*>(arg), xsink);
            if (*xsink || !array)
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                SQL_DOUBLE, DOUBLE_COLSIZE, 0, array, sizeof(double), 0);
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
            void* array = createArrayFromBinary(reinterpret_cast<const BinaryNode*>(arg), len, xsink);
            if (*xsink || !array)
                return -1;
            SQLLEN* indArray = createIndArray(len, xsink);
            if (*xsink || !indArray)
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

int ODBCStatement::bindParamArrayBindHash(int column, const QoreHashNode* h, ExceptionSink* xsink) {
    SQLRETURN ret;
    const AbstractQoreNode* odbct = h->getKeyValue("^odbct^");
    const AbstractQoreNode* value = h->getKeyValue("^value^");
    const QoreBigIntNode* odbcType = reinterpret_cast<const QoreBigIntNode*>(odbct);
    switch (odbcType->val) {
        case SQL_TYPE_DATE:
            if (bindTypeDateArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_TYPE_TIME:
            if (bindTypeTimeArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_TYPE_TIMESTAMP:
            if (bindTypeTimestampArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_MONTH:
            if (bindTypeIntMonthArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_YEAR:
            if (bindTypeIntYearArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_YEAR_TO_MONTH:
            if (bindTypeIntMonthArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_DAY:
            if (bindTypeIntDayArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_HOUR:
            if (bindTypeIntHourArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_MINUTE:
            if (bindTypeIntMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_SECOND:
            if (bindTypeIntSecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_DAY_TO_HOUR:
            if (bindTypeIntDayHourArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_DAY_TO_MINUTE:
            if (bindTypeIntDayMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_DAY_TO_SECOND:
            if (bindTypeIntDaySecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_HOUR_TO_MINUTE:
            if (bindTypeIntHourMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_HOUR_TO_SECOND:
            if (bindTypeIntHourSecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_INTERVAL_MINUTE_TO_SECOND:
            if (bindTypeIntMinuteSecondArray(column, value, ret, xsink))
                return -1;
            break;
        default:
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "odbc_bind used with an unknown type identifier: %d", odbcType->val);
            return -1;
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array column #%d");
        ErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:BIND-ERROR", s.c_str(), column);
        return -1;
    }

    return 0;
}

int ODBCStatement::bindTypeDate(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_DATE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isRelative()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_DATE odbc_bind");
        return -1;
    }

    DATE_STRUCT d;
    qore_tm info;
    info.clear();
    date->getInfo(info);
    d.year = info.year;
    d.month = info.month;
    d.day = info.day;
    DATE_STRUCT* dval = paramHolder.addDate(d);
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
        SQL_TYPE_DATE, TYPE_DATE_COLSIZE, 0, dval, sizeof(DATE_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTime(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_TIME odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isRelative()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_TIME odbc_bind");
        return -1;
    }

    TIME_STRUCT t;
    qore_tm info;
    info.clear();
    date->getInfo(info);
    t.hour = info.hour;
    t.minute = info.minute;
    t.second = info.second;
    TIME_STRUCT* tval = paramHolder.addTime(t);
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
        SQL_TYPE_TIME, TYPE_TIME_COLSIZE, 0, tval, sizeof(TIME_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTimestamp(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_TIMESTAMP odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isRelative()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_TIMESTAMP odbc_bind");
        return -1;
    }

    TIMESTAMP_STRUCT* tval = paramHolder.addTimestamp(getTimestampFromDate(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
        SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, tval, sizeof(TIMESTAMP_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYear(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_YEAR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEAR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getYearInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR,
        SQL_INTERVAL_YEAR, INT_YEAR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMonth(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_MONTH odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MONTH odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMonthInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MONTH,
        SQL_INTERVAL_MONTH, INT_MONTH_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYearMonth(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_YEARMONTH odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEARMONTH odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getYearMonthInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR_TO_MONTH,
        SQL_INTERVAL_YEAR_TO_MONTH, INT_YEARMONTH_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDay(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_DAY odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAY odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY,
        SQL_INTERVAL_DAY, INT_DAY_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHour(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_HOUR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOUR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR,
        SQL_INTERVAL_HOUR, INT_HOUR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_MINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE,
        SQL_INTERVAL_MINUTE, INT_MINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_SECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_SECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_SECOND,
        SQL_INTERVAL_SECOND, INT_SECOND_COLSIZE, 9, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayHour(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_DAYHOUR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYHOUR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayHourInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_HOUR,
        SQL_INTERVAL_DAY_TO_HOUR, INT_DAYHOUR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_DAYMINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYMINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_MINUTE,
        SQL_INTERVAL_DAY_TO_MINUTE, INT_DAYMINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDaySecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_DAYSECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYSECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDaySecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
        SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourMinute(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_HOURMINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURMINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_MINUTE,
        SQL_INTERVAL_HOUR_TO_MINUTE, INT_HOURMINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_HOURSECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURSECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_SECOND,
        SQL_INTERVAL_HOUR_TO_SECOND, INT_HOURSECOND_COLSIZE, 9, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinuteSecond(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg->getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value passed with ODBCT_INT_MINUTESECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
    if (date->isAbsolute()) {
        xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTESECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMinuteSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE_TO_SECOND,
        SQL_INTERVAL_MINUTE_TO_SECOND, INT_MINUTESECOND_COLSIZE, 9, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeDateArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        DATE_STRUCT* array = arrayHolder.addDateArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }
        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            DATE_STRUCT d;
            qore_tm info;
            info.clear();
            date->getInfo(date->getZone(), info);
            d.year = info.year;
            d.month = info.month;
            d.day = info.day;
            array[i] = d;
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
            SQL_TYPE_DATE, TYPE_DATE_COLSIZE, 0, array, sizeof(DATE_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isRelative()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_DATE odbc_bind");
            return -1;
        }
        DATE_STRUCT* array = arrayHolder.addDateArray(xsink);
        if (!array)
            return -1;

        DATE_STRUCT d;
        qore_tm info;
        info.clear();
        date->getInfo(date->getZone(), info);
        d.year = info.year;
        d.month = info.month;
        d.day = info.day;
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = d;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
            SQL_TYPE_DATE, TYPE_DATE_COLSIZE, 0, array, sizeof(DATE_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_DATE odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeTimeArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        TIME_STRUCT* array = arrayHolder.addTimeArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }
        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            TIME_STRUCT t;
            qore_tm info;
            info.clear();
            date->getInfo(date->getZone(), info);
            t.hour = info.hour;
            t.minute = info.minute;
            t.second = info.second;
            array[i] = t;
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
            SQL_TYPE_TIME, TYPE_TIME_COLSIZE, 0, array, sizeof(TIME_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isRelative()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_TIME odbc_bind");
            return -1;
        }
        TIME_STRUCT* array = arrayHolder.addTimeArray(xsink);
        if (!array)
            return -1;

        TIME_STRUCT t;
        qore_tm info;
        info.clear();
        date->getInfo(date->getZone(), info);
        t.hour = info.hour;
        t.minute = info.minute;
        t.second = info.second;
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = t;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
            SQL_TYPE_TIME, TYPE_TIME_COLSIZE, 0, array, sizeof(TIME_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_TIME odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeTimestampArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        TIMESTAMP_STRUCT* array = arrayHolder.addTimestampArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }
        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            TIMESTAMP_STRUCT t = getTimestampFromDate(date);
            array[i] = t;
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, array, sizeof(TIMESTAMP_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isRelative()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "a relative date value passed with ODBCT_TIMESTAMP odbc_bind");
            return -1;
        }
        TIMESTAMP_STRUCT* array = arrayHolder.addTimestampArray(xsink);
        if (!array)
            return -1;

        TIMESTAMP_STRUCT t = getTimestampFromDate(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = t;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP, TYPE_TIMESTAMP_COLSIZE, 9, array, sizeof(TIMESTAMP_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_TIMESTAMP odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntYearArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getYearInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR,
            SQL_INTERVAL_YEAR, INT_YEAR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEAR odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getYearInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR,
            SQL_INTERVAL_YEAR, INT_YEAR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_YEAR odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntMonthArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getMonthInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MONTH,
            SQL_INTERVAL_MONTH, INT_MONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MONTH odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getMonthInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MONTH,
            SQL_INTERVAL_MONTH, INT_MONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MONTH odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntYearMonthArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getYearMonthInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR_TO_MONTH,
            SQL_INTERVAL_YEAR_TO_MONTH, INT_YEARMONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEARMONTH odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getYearMonthInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR_TO_MONTH,
            SQL_INTERVAL_YEAR_TO_MONTH, INT_YEARMONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_YEARMONTH odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntDayArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getDayInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY,
            SQL_INTERVAL_DAY, INT_DAY_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAY odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getDayInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY,
            SQL_INTERVAL_DAY, INT_DAY_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAY odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntHourArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getHourInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR,
            SQL_INTERVAL_HOUR, INT_HOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOUR odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getHourInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR,
            SQL_INTERVAL_HOUR, INT_HOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOUR odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getMinuteInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE,
            SQL_INTERVAL_MINUTE, INT_MINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTE odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getMinuteInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE,
            SQL_INTERVAL_MINUTE, INT_MINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MINUTE odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getSecondInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_SECOND,
            SQL_INTERVAL_SECOND, INT_SECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_SECOND odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getSecondInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_SECOND,
            SQL_INTERVAL_SECOND, INT_SECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_SECOND odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntDayHourArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getDayHourInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_HOUR,
            SQL_INTERVAL_DAY_TO_HOUR, INT_DAYHOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYHOUR odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getDayHourInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_HOUR,
            SQL_INTERVAL_DAY_TO_HOUR, INT_DAYHOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYHOUR odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntDayMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getDayMinuteInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_MINUTE,
            SQL_INTERVAL_DAY_TO_MINUTE, INT_DAYMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYMINUTE odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getDayMinuteInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_MINUTE,
            SQL_INTERVAL_DAY_TO_MINUTE, INT_DAYMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYMINUTE odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntDaySecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getDaySecondInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
            SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYSECOND odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getDaySecondInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
            SQL_INTERVAL_DAY_TO_SECOND, INT_DAYSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYSECOND odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntHourMinuteArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getHourMinuteInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_MINUTE,
            SQL_INTERVAL_HOUR_TO_MINUTE, INT_HOURMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURMINUTE odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getHourMinuteInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_MINUTE,
            SQL_INTERVAL_HOUR_TO_MINUTE, INT_HOURMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOURMINUTE odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntHourSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getHourSecondInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_SECOND,
            SQL_INTERVAL_HOUR_TO_SECOND, INT_HOURSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURSECOND odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getHourSecondInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_SECOND,
            SQL_INTERVAL_HOUR_TO_SECOND, INT_HOURSECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOURSECOND odbc_bind");
    return -1;
}

int ODBCStatement::bindTypeIntMinuteSecondArray(int column, const AbstractQoreNode* arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg->getType();
    qore_size_t arraySize = arrayHolder.getArraySize();
    if (argtype == NT_LIST) {
        const QoreListNode* lst = reinterpret_cast<const QoreListNode*>(arg);
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;
        if (lst->size() != arraySize) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR",
                "mismatch between the list size and required number of list elements (column #%d); %u required, %u passed",
                column, arraySize, lst->size());
            return -1;
        }

        for (qore_size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(lst->retrieve_entry(i));
            array[i] = getMinuteSecondInterval(date);
        }
        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE_TO_SECOND,
            SQL_INTERVAL_MINUTE_TO_SECOND, INT_MINUTESECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }
    else if (argtype == NT_DATE) {
        const DateTimeNode* date = reinterpret_cast<const DateTimeNode*>(arg);
        if (date->isAbsolute()) {
            xsink->raiseException("DBI:ODBC:BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTESECOND odbc_bind");
            return -1;
        }
        SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
        if (!array)
            return -1;

        SQL_INTERVAL_STRUCT interval = getMinuteSecondInterval(date);
        for (qore_size_t i = 0; i < arraySize; i++) {
            array[i] = interval;
        }

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE_TO_SECOND,
            SQL_INTERVAL_MINUTE_TO_SECOND, INT_MINUTESECOND_COLSIZE, 9, array, sizeof(SQL_INTERVAL_STRUCT), 0);
        return 0;
    }

    xsink->raiseException("DBI:ODBC:BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MINUTESECOND odbc_bind");
    return -1;
}

int ODBCStatement::createArrayFromStringList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
    char** stringArray = arrayHolder.addCharArray(xsink);
    if (!stringArray)
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
        stringArray[i] = getCharsFromString(str, len, xsink);
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }

    // We have to create one big array and put all the strings in it one after another.
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*maxlen);
        return -1;
    }
    for (qore_size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            array[i*maxlen] = '\0';
            continue;
        }
        memcpy((array + i*maxlen), stringArray[i], indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromNumberList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
    char** stringArray = arrayHolder.addCharArray(xsink);
    if (!stringArray)
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
        stringArray[i] = vh.giveBuffer();
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }

    // We have to create one big array and put all the strings in it one after another.
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*maxlen);
        return -1;
    }
    for (qore_size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            array[i*maxlen] = '\0';
            continue;
        }
        memcpy((array + i*maxlen), stringArray[i], indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromBinaryList(const QoreListNode* arg, void*& array, SQLLEN*& indArray, qore_size_t& maxlen, ExceptionSink* xsink) {
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
        indArray[i] = bin->size();
        maxlen = (maxlen >= static_cast<qore_size_t>(indArray[i])) ? maxlen : indArray[i];
    }

    // We have to create one big array and put all the binaries in it one after another (kind of very inefficient).
    char* charArray = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    array = static_cast<void*>(charArray);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*maxlen);
        return -1;
    }
    for (qore_size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            charArray[i*maxlen] = 0;
            continue;
        }
        const BinaryNode* bin = reinterpret_cast<const BinaryNode*>(arg->retrieve_entry(i));
        memcpy((charArray + i*maxlen), bin->getPtr(), indArray[i]);
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

char* ODBCStatement::createArrayFromString(const QoreStringNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    qore_size_t arraySize = arrayHolder.getArraySize();
    char* val = paramHolder.addChars(getCharsFromString(arg, len, xsink));
    if (!val)
        return 0;
    char* array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*len);
        return 0;
    }
    for (qore_size_t i = 0; i < arraySize; i++)
        memcpy((array + i*len), val, len);

    return array;
}

char* ODBCStatement::createArrayFromNumber(const QoreNumberNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
    if (*xsink)
        return 0;
    len = vh->strlen();
    qore_size_t arraySize = arrayHolder.getArraySize();
    char* val = paramHolder.addChars(vh.giveBuffer());
    char* array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*len);
        return 0;
    }
    for (qore_size_t i = 0; i < arraySize; i++)
        memcpy((array + i*len), val, len);

    return array;
}

void* ODBCStatement::createArrayFromBinary(const BinaryNode* arg, qore_size_t& len, ExceptionSink* xsink) {
    len = arg->size();
    qore_size_t arraySize = arrayHolder.getArraySize();
    void* val = const_cast<void*>(arg->getPtr());
    char* array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array with size of %d bytes", arraySize*len);
        return 0;
    }
    for (qore_size_t i = 0; i < arraySize; i++)
        memcpy((array + i*len), val, len);

    return static_cast<void*>(array);
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

SQLLEN* ODBCStatement::createIndArray(SQLLEN indicator, ExceptionSink* xsink) {
    SQLLEN* array = arrayHolder.addIndArray(xsink);
    if (!array)
        return 0;
    qore_size_t arraySize = arrayHolder.getArraySize();
    for (qore_size_t i = 0; i < arraySize; i++)
        array[i] = indicator;
    return array;
}
