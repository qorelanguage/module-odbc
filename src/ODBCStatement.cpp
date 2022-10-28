/* -*- indent-tabs-mode: nil -*- */
/*
  ODBCStatement.cpp

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

#include "ODBCStatement.h"

#include <climits>

#include "qore/QoreLib.h"
#include "qore/DBI.h"

#include "ODBCColumnSizeConstants.h"
#include "ODBCConnection.h"

namespace odbc {

/////////////////////////////
//     Public methods     //
///////////////////////////

ODBCStatement::ODBCStatement(ODBCConnection* c, ExceptionSink* xsink) :
        bindArgs(xsink),
        conn(c),
        serverTz(conn->getServerTimezone()),
        options(conn->getOptions()),
        params(new QoreListNode, xsink) {
    if (conn->allocStatementHandle(stmt, xsink)) {
        return;
    }
}

ODBCStatement::ODBCStatement(Datasource* ds, ExceptionSink* xsink) :
        bindArgs(xsink),
        conn(static_cast<ODBCConnection*>(ds->getPrivateData())),
        serverTz(conn->getServerTimezone()),
        options(conn->getOptions()),
        params(new QoreListNode, xsink) {
    if (conn->allocStatementHandle(stmt, xsink))
        return;
}

ODBCStatement::~ODBCStatement() {
    freeStatementHandle();
}

bool ODBCStatement::hasResultData() {
    SQLSMALLINT columns = 0;
    SQLRETURN ret = SQLNumResultCols(stmt, &columns);
    if (!SQL_SUCCEEDED(ret)) { // error
        return false;
    }
    return columns > 0;
}

const QoreEncoding* ODBCStatement::getQoreEncoding() const {
    return conn->getDatasource()->getQoreEncoding();
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

        ReferenceHolder<QoreHashNode> desc(new QoreHashNode(autoTypeInfo), xsink);
        desc->setKeyValue(namestr, new QoreStringNode(col.name), xsink);
        desc->setKeyValue(internalstr, (int64)col.dataType, xsink);
        desc->setKeyValue(maxsizestr, (int64)col.byteSize, xsink);

        switch (col.dataType) {
            // Integer types.
            case SQL_INTEGER:
                desc->setKeyValue(typestr, NT_INT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTEGER"), xsink);
                break;
            case SQL_BIGINT:
                desc->setKeyValue(typestr, NT_INT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BIGINT"), xsink);
                break;
            case SQL_SMALLINT:
                desc->setKeyValue(typestr, NT_INT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_SMALLINT"), xsink);
                break;
            case SQL_TINYINT:
                desc->setKeyValue(typestr, NT_INT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TINYINT"), xsink);
                break;

            // Float types.
            case SQL_FLOAT:
                desc->setKeyValue(typestr, NT_FLOAT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_FLOAT"), xsink);
                break;
            case SQL_DOUBLE:
                desc->setKeyValue(typestr, NT_FLOAT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_DOUBLE"), xsink);
                break;
            case SQL_REAL:
                desc->setKeyValue(typestr, NT_FLOAT, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_REAL"), xsink);
                break;

            // Character types.
            case SQL_CHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_CHAR"), xsink);
                break;
            case SQL_VARCHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_VARCHAR"), xsink);
                break;
            case SQL_LONGVARCHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_LONGVARCHAR"), xsink);
                break;
            case SQL_WCHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WCHAR"), xsink);
                break;
            case SQL_WVARCHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WVARCHAR"), xsink);
                break;
            case SQL_WLONGVARCHAR:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_WLONGVARCHAR"), xsink);
                break;

            // Binary types.
            case SQL_BINARY:
                desc->setKeyValue(typestr, NT_BINARY, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BINARY"), xsink);
                break;
            case SQL_VARBINARY:
                desc->setKeyValue(typestr, NT_BINARY, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_VARBINARY"), xsink);
                break;
            case SQL_LONGVARBINARY:
                desc->setKeyValue(typestr, NT_BINARY, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_LONGVARBINARY"), xsink);
                break;

            // Various.
            case SQL_BIT:
                desc->setKeyValue(typestr, NT_BOOLEAN, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_BIT"), xsink);
                break;
            case SQL_NUMERIC:
                desc->setKeyValue(typestr, NT_NUMBER, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_NUMERIC"), xsink);
                break;
            case SQL_DECIMAL:
                desc->setKeyValue(typestr, NT_NUMBER, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_DECIMAL"), xsink);
                break;

            // Time types.
            case SQL_TYPE_TIMESTAMP:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_TIMESTAMP"), xsink);
                break;
            case SQL_TYPE_TIME:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_TIME"), xsink);
                break;
            case SQL_TYPE_DATE:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_TYPE_DATE"), xsink);
                break;

            // Interval types.
            case SQL_INTERVAL_MONTH:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MONTH"), xsink);
                break;
            case SQL_INTERVAL_YEAR:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_YEAR"), xsink);
                break;
            case SQL_INTERVAL_YEAR_TO_MONTH:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_YEAR_TO_MONTH"), xsink);
                break;
            case SQL_INTERVAL_DAY:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY"), xsink);
                break;
            case SQL_INTERVAL_HOUR:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR"), xsink);
                break;
            case SQL_INTERVAL_MINUTE:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_SECOND:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_SECOND"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_HOUR:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_HOUR"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_MINUTE:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_DAY_TO_SECOND:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_DAY_TO_SECOND"), xsink);
                break;
            case SQL_INTERVAL_HOUR_TO_MINUTE:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR_TO_MINUTE"), xsink);
                break;
            case SQL_INTERVAL_HOUR_TO_SECOND:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_HOUR_TO_SECOND"), xsink);
                break;
            case SQL_INTERVAL_MINUTE_TO_SECOND:
                desc->setKeyValue(typestr, NT_DATE, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_INTERVAL_MINUTE_TO_SECOND"), xsink);
                break;
            case SQL_GUID:
                desc->setKeyValue(typestr, NT_STRING, xsink);
                desc->setKeyValue(dbtypestr, new QoreStringNode("SQL_GUID"), xsink);
                break;
        } // switch

        hah.assign(desc.release(), xsink);
    }

    return h.release();
}

void ODBCStatement::freeStatementHandle() {
    if (stmt != SQL_NULL_HSTMT) {
        SQLCloseCursor(stmt);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        stmt = SQL_NULL_HSTMT;
    }
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

    ReferenceHolder<QoreHashNode> h(new QoreHashNode(autoTypeInfo), xsink);
    std::vector<QoreListNode*> columns;

    int rowCount = 0;
    while (true) {
        SQLRETURN ret = SQLFetch(stmt);
        if (ret == SQL_NO_DATA) { // Reached the end of the result-set.
            break;
        }
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching row #%d");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("ODBC-FETCH-ERROR", s.c_str(), readRows);
            return 0;
        }

        int columnCount = resColumns.size();
        for (int j = 0; j < columnCount; j++) {
            ODBCResultColumn& rcol = resColumns[j];
            ValueHolder n(getColumnValue(j+1, rcol, xsink), xsink);
            if (!n || *xsink) {
                return 0;
            }

            if (h->empty()) {
               populateColumnHash(**h, columns);
            }

            (columns[j])->push(n.release(), xsink);
        }
        readRows++;
        rowCount++;
        if (rowCount == maxRows && maxRows > 0) {
            break;
        }
    }

    if (!rowCount && !emptyHashIfNothing)
       populateColumnHash(**h, columns);

    return h.release();
}

QoreListNode* ODBCStatement::getOutputList(ExceptionSink* xsink, int maxRows) {
    if (fetchResultColumnMetadata(xsink))
        return 0;

    ReferenceHolder<QoreListNode> l(new QoreListNode(autoTypeInfo), xsink);

    int rowCount = 0;
    GetRowInternStatus status;
    while (true) {
        ReferenceHolder<QoreHashNode> h(getRowIntern(status, xsink), xsink);
        if (status == EGRIS_OK) { // Ok.
            l->push(h.release(), xsink);
            rowCount++;
            if (rowCount == maxRows && maxRows > 0)
                break;
        } else if (status == EGRIS_END) { // End of result-set.
            break;
        } else { // status == EGRIS_ERROR
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
            xsink->raiseException("ODBC-SELECT-ROW-ERROR", "SQL passed to selectRow() returned more than 1 row");
            return 0;
        }
        if (status == EGRIS_ERROR)
            return 0;
    } else if (status == EGRIS_END || status == EGRIS_ERROR) { // No data or error.
        return 0;
    }

    return h.release();
}

int ODBCStatement::exec(const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    command = qstr;

    // Convert string to required character encoding.
    TempEncodingHelper str(qstr, getQoreEncoding(), xsink);
    if (*xsink) {
        return -1;
    }
    str.makeTemp();
    if (parse(const_cast<QoreString*>(*str), args, xsink)) {
        return -1;
    }

    if (hasArrays(args)) {
        if (bindInternArray(*params, xsink)) {
            return -1;
        }
    } else {
        if (bindIntern(*params, xsink)) {
            return -1;
        }
    }

    SQLINTEGER textLen = str->length();
    return execIntern(str->c_str(), textLen, xsink);
}

int ODBCStatement::exec(const QoreString* qstr, ExceptionSink* xsink) {
    command = qstr;

    // Convert string to required character encoding.
    TempEncodingHelper str(qstr, getQoreEncoding(), xsink);
    if (*xsink) {
        return -1;
    }

    SQLINTEGER textLen = str->length();
    return execIntern(str->c_str(), textLen, xsink);
}

void ODBCStatement::clear(ExceptionSink* xsink) {
    // Free current statement handle since the associated connection handle has to be also freed.
    freeStatementHandle();
}

void ODBCStatement::reset(ExceptionSink* xsink) {
    clear(xsink);

    affectedRowCount = 0;
    readRows = 0;
    paramCountInSql = 0;

    paramHolder.clear();
    arrayHolder.clear();
    resColumns.clear();

    params = nullptr;
    bindArgs = nullptr;
}

/////////////////////////////
//   Protected methods    //
///////////////////////////

void ODBCStatement::handleStmtError(const char* err, const char* desc, ExceptionSink* xsink) {
    std::string s(desc);
    ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
    xsink->raiseException(err, s.c_str());
}

int ODBCStatement::resetAfterLostConnection(ExceptionSink* xsink) {
    // Convert string to required character encoding.
    TempEncodingHelper str(command, conn->getDatasource()->getQoreEncoding(), xsink);
    if (*xsink) {
        return -1;
    }
    str.makeTemp();

    // Re-parse command and arguments.
    if (parse(const_cast<QoreString*>(*str), *bindArgs, xsink)) {
        return -1;
    }

    // We need an empty xsink for the binding functions.
    ExceptionSink xs;

    // Re-bind parameters.
    if (hasArrays(*bindArgs)) {
        if (bindInternArray(*params, &xs)) {
            xsink->assimilate(xs);
            return -1;
        }
    } else {
        if (bindIntern(*params, &xs)) {
            xsink->assimilate(xs);
            return -1;
        }
    }

    return 0;
}

int ODBCStatement::execIntern(const char* str, SQLINTEGER textLen, ExceptionSink* xsink) {
    //fprintf(stderr, "exec: '%s'\non connection: %p\n", command.c_str(), conn);
    SQLRETURN ret;
    if (str) {
        ret = SQLExecDirectA(stmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(str)), textLen);
    } else {
        ret = SQLExecute(stmt);
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        char state[7];
        memset(state, 0, sizeof(state));
        ODBCErrorHelper::extractState(SQL_HANDLE_STMT, stmt, state);
        if (strncmp(state, "08S01", 5) == 0) { // 08S01 == Communication link failure (aka lost connection).
            if (conn->getDatasource()->activeTransaction()) {
                xsink->raiseException("ODBC-TRANSACTION-ERROR", "connection to database server lost while in a "
                    "transaction; transaction has been lost");
            }

            // Reset current statement state while the driver-specific context data is still present.
            clear(xsink);

            // Free and reset statement states for all active statements while the driver-specific context data is still present.
            conn->getDatasource()->connectionLost(xsink);

            // Disconnect first.
            conn->disconnect();

            // Then try to reconnect.
            if (conn->connect(xsink)) {
                // Free state completely.
                reset(xsink);

                // Reconnect failed. Marking connection as closed.
                // The following call will close any open statements and then the datasource.
                conn->getDatasource()->connectionAborted();
                return -1;
            }

            // Don't execute again if the connection was aborted while in a transaction.
            if (conn->getDatasource()->activeTransaction()) {
                return -1;
            }

            // Don't execute again if any exceptions have occured, including if the connection was aborted while in a transaction.
            if (*xsink) {
                // Close all statements and remove private data but leave datasource open.
                conn->getDatasource()->connectionRecovered(xsink);
                return -1;
            }

            // Allocate new statement handle.
            if (conn->allocStatementHandle(stmt, xsink)) {
                return -1;
            }

            // Reset and prepare everything before re-execution.
            if (resetAfterLostConnection(xsink)) {
                return -1;
            }

#ifdef DEBUG
            // Otherwise show the exception on stdout in debug mode.
            //xsink->handleExceptions();
#endif
            // Clear any exceptions that have been ignored.
            xsink->clear();

            // Re-execute.
            if (str) {
                ret = SQLExecDirectA(stmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(str)), textLen);
            } else {
                ret = SQLExecute(stmt);
            }
            if (!SQL_SUCCEEDED(ret)) { // error
                QoreStringMaker err("error in statement execution (sql: '%s', ret: %d)", str ? str : "n/a", (int)ret);
                handleStmtError("ODBC-EXEC-ERROR", err.c_str(), xsink);
                affectedRowCount = -1;
                return -1;
            }
        } else { // Exec failed but for some other reason than lost connection.
            QoreStringMaker err("error in statement execution (sql: '%s', ret: %d)", str ? str : "n/a", (int)ret);
            handleStmtError("ODBC-EXEC-ERROR", err.c_str(), xsink);
            affectedRowCount = -1;
            return -1;
        }
    }

    // Get count of affected rows.
    SQLLEN len;
    ret = SQLRowCount(stmt, &len);
    if (SQL_SUCCEEDED(ret)) {
        affectedRowCount = len;
    } else {  // error
        affectedRowCount = -1;
    }

    return 0;
}

int ODBCStatement::parse(QoreString* str, const QoreListNode* args, ExceptionSink* xsink) {
    char quote = 0;
    const char *p = str->c_str();
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
            } else {
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

            if ((*p) == '%' && (p == str->c_str() || !isalnum(*(p-1)))) { // Found value marker.
                int offset = p - str->c_str();

                p++;
                QoreValue v = args ? args->retrieveEntry(index++) : QoreValue();
                if ((*p) == 'd') {
                    DBI_concat_numeric(&tmp, v);
                    str->replace(offset, 2, tmp.c_str());
                    p = str->c_str() + offset + tmp.strlen();
                    tmp.clear();
                    continue;
                }
                if ((*p) == 's') {
                    if (DBI_concat_string(&tmp, v, xsink))
                        return -1;
                    str->replace(offset, 2, tmp.c_str());
                    p = str->c_str() + offset + tmp.strlen();
                    tmp.clear();
                    continue;
                }
                if ((*p) != 'v') {
                    xsink->raiseException("ODBC-PARSE-ERROR", "invalid value specification (expecting '%v' or '%%d', "
                        "got %%%c)", *p);
                    return -1;
                }
                p++;
                if (isalpha(*p)) {
                    xsink->raiseException("ODBC-PARSE-ERROR", "invalid value specification (expecting '%v' or '%%d', "
                        "got %%v%c*)", *p);
                    return -1;
                }

                str->replace(offset, 2, "?");
                p = str->c_str() + offset + 1;
                paramCountInSql++;
                if (v) {
                    params->push(v.refSelf(), xsink);
                }
                continue;
            }

            // Allow escaping of '%' characters.
            if ((*p) == '\\' && (*(p+1) == ':' || *(p+1) == '%')) {
                str->splice(p - str->c_str(), 1, xsink);
                p += 2;
                continue;
            }
        }

        if (((*p) == '\'') || ((*p) == '\"')) {
            if (!quote) {
                quote = *p;
            } else if (quote == (*p)) {
                quote = 0;
            }
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
        return nullptr;
    }
    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("error occured when fetching row #%d");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("ODBC-FETCH-ERROR", s.c_str(), readRows);
        status = EGRIS_ERROR;
        return nullptr;
    }

    // issue #4616: fetch column metadata if necessary
    if (!resColumns.size() && fetchResultColumnMetadata(xsink)) {
        return nullptr;
    }

    ReferenceHolder<QoreHashNode> h(new QoreHashNode(autoTypeInfo), xsink); // Row hash.

    int columns = resColumns.size();
    for (int i = 0; i < columns; ++i) {
        ODBCResultColumn& col = resColumns[i];
        ValueHolder n(xsink);
        n = getColumnValue(i + 1, col, xsink);
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
    ++readRows;

    status = EGRIS_OK;
    return h.release();
}

bool ODBCStatement::hasArrays(const QoreListNode* args) const {
    return findArraySizeOfArgs(args) > 0;
}

size_t ODBCStatement::findArraySizeOfArgs(const QoreListNode* args) const {
    size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        QoreValue arg = args->retrieveEntry(i);
        qore_type_t ntype = arg.getType();
        if (ntype == NT_LIST)
            return arg.get<const QoreListNode>()->size();
    }
    return 0;
}

int ODBCStatement::bindIntern(const QoreListNode* args, ExceptionSink* xsink) {
    // Clear previous parameters.
    arrayHolder.clear();
    paramHolder.clear();

    // Any parameters without a bind argument will be bound as null
    // Set parameter array size to 1.
    size_t one = 1;
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(one), 0);

    size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        QoreValue arg = args->retrieveEntry(i);
        SQLRETURN ret;

        if (arg.isNullOrNothing()) { // Bind NULL argument.
            SQLLEN* len = paramHolder.addLength(SQL_NULL_DATA);
            ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 0, 0, 0, 0, len);
            if (!SQL_SUCCEEDED(ret)) { // error
                std::string s("failed binding NULL parameter with index %u (column %u)");
                ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
                xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), i, i + 1);
                return -1;
            }
            continue;
        }

        qore_type_t ntype = arg.getType();
        switch (ntype) {
            case NT_STRING: {
                const QoreStringNode* str = arg.get<const QoreStringNode>();
                size_t len;
                char* cstr = paramHolder.addChars(getCharsFromString(str, len, xsink));
                if (*xsink)
                    return -1;
                SQLLEN* indPtr = paramHolder.addLength(len);
                ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                    len, 0, reinterpret_cast<SQLCHAR*>(cstr), len, indPtr);
                break;
            }
            case NT_NUMBER: {
                QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
                if (*xsink)
                    return -1;
                size_t len = vh->strlen();
                SQLLEN* indPtr = paramHolder.addLength(len);
                char* cstr = paramHolder.addChars(vh.giveBuffer());
                ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, cstr, len, indPtr);
                break;
            }
            case NT_DATE: {
                const DateTimeNode* date = arg.get<const DateTimeNode>();
                if (date->isAbsolute()) {
                    TIMESTAMP_STRUCT* tval = paramHolder.addTimestamp(getTimestampFromDate(date));
                    ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
                        getTimestampColsize(options), options.frPrec, tval, sizeof(TIMESTAMP_STRUCT), 0);
                } else {
                    SQL_INTERVAL_STRUCT* tval = paramHolder.addInterval(getIntervalFromDate(date));
                    ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                        SQL_INTERVAL_DAY_TO_SECOND,
                        getIntDaySecondColsize(options), options.frPrec, tval, sizeof(SQL_INTERVAL_STRUCT), 0);
                }
                break;
            }
            case NT_INT: {
                if (options.bigint == EBO_NATIVE) {
                    const int64* ival = paramHolder.addInt64(arg.getAsBigInt());
                    ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                        SQL_BIGINT, BIGINT_COLSIZE, 0, const_cast<int64*>(ival), sizeof(int64), 0);
                } else if (options.bigint == EBO_STRING) {
                    QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
                    if (*xsink) {
                        return -1;
                    }
                    size_t len = vh->strlen();
                    SQLLEN* indPtr = paramHolder.addLength(len);
                    char* cstr = paramHolder.addChars(vh.giveBuffer());
                    ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, cstr, len, indPtr);
                }
                break;
            }
            case NT_FLOAT: {
                const double* f = paramHolder.addDouble(arg.getAsFloat());
                ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                    SQL_DOUBLE, DOUBLE_COLSIZE, 0, const_cast<double*>(f), sizeof(double), 0);
                break;
            }
            case NT_BOOLEAN: {
                bool b = arg.getAsBool();
                int8_t* bval = paramHolder.addInt8(b);
                ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_STINYINT,
                    SQL_TINYINT, 3, 0, bval, sizeof(int8_t), 0);
                break;
            }
            case NT_BINARY: {
                const BinaryNode* b = arg.get<const BinaryNode>();
                size_t len = b->size();
                SQLLEN* indPtr = paramHolder.addLength(len);
                ret = SQLBindParameter(stmt, i + 1, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQL_BINARY, len, 0, const_cast<void*>(b->getPtr()), len, indPtr);
                break;
            }
            case NT_HASH: {
                const QoreHashNode* h = arg.get<const QoreHashNode>();
                QoreValue odbct = h->getKeyValue("^odbct^");
                QoreValue value = h->getKeyValue("^value^");
                if (!value || odbct.getType() != NT_INT) {
                    xsink->raiseException("ODBC-BIND-ERROR", "hash parameter not in correct format for odbc_bind; "
                        "the odbc module cannot bind hash values");
                    return -1;
                }
                int64 odbcType = odbct.getAsBigInt();
                switch (odbcType) {
                    case SQL_C_SLONG:
                        if (bindTypeSLong(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_ULONG:
                        if (bindTypeULong(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_SSHORT:
                        if (bindTypeSShort(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_USHORT:
                        if (bindTypeUShort(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_STINYINT:
                        if (bindTypeSTinyint(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_UTINYINT:
                        if (bindTypeUTinyint(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_FLOAT:
                        if (bindTypeFloat(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_TYPE_DATE:
                        if (bindTypeDate(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_TYPE_TIME:
                        if (bindTypeTime(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_TYPE_TIMESTAMP:
                        if (bindTypeTimestamp(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_MONTH:
                        if (bindTypeIntMonth(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_YEAR:
                        if (bindTypeIntYear(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_YEAR_TO_MONTH:
                        if (bindTypeIntMonth(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_DAY:
                        if (bindTypeIntDay(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_HOUR:
                        if (bindTypeIntHour(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_MINUTE:
                        if (bindTypeIntMinute(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_SECOND:
                        if (bindTypeIntSecond(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_DAY_TO_HOUR:
                        if (bindTypeIntDayHour(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_DAY_TO_MINUTE:
                        if (bindTypeIntDayMinute(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_DAY_TO_SECOND:
                        if (bindTypeIntDaySecond(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_HOUR_TO_MINUTE:
                        if (bindTypeIntHourMinute(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_HOUR_TO_SECOND:
                        if (bindTypeIntHourSecond(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    case SQL_C_INTERVAL_MINUTE_TO_SECOND:
                        if (bindTypeIntMinuteSecond(i + 1, value, ret, xsink))
                            return -1;
                        break;
                    default:
                        xsink->raiseException("ODBC-BIND-ERROR", "odbc_bind used with an unknown type identifier: "
                            QLLD, odbcType);
                        return -1;
                }
            }
            default: {
                xsink->raiseException("ODBC-BIND-ERROR", "do not know how to bind values of type '%s'",
                    arg.getTypeName());
                return -1;
            }
        } // switch

        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("failed binding parameter with index %u (column #%u) of type '%s'");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), i, i + 1, arg.getTypeName());
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
        xsink->raiseException("ODBC-BIND-ERROR",
            "mismatch between the parameter list size and number of parameters in the SQL command; %lu "
            "required, %lu passed", paramCountInSql, args->size());
        return -1;
    }

    // Find parameter array size.
    size_t arraySize = findArraySizeOfArgs(args);
    arrayHolder.setArraySize(arraySize);

    // Use column-wise binding.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, SQL_PARAM_BIND_BY_COLUMN, 0);

    // Set parameter array size.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(arraySize), 0);

    // Specify an array in which to return the status of each set of parameters.
    // Since this is unnecessarily precise status reporting, we set it to 0,
    // so that these states are not generated.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, nullptr, 0);

    // Specify an SQLUINTEGER value in which to return the number of sets of parameters processed.
    // Also unnecessary information, therefore set to 0.
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, nullptr, 0);

    size_t count = args ? args->size() : 0;
    for (unsigned int i = 0; i < count; i++) {
        QoreValue arg = args->retrieveEntry(i);

        if (arg.isNullOrNothing()) { // Handle NULL argument.
            bindParamArraySingleValue(i + 1, arg, xsink);
            continue;
        }

        qore_type_t ntype = arg.getType();
        switch (ntype) {
            case NT_LIST:
                if (bindParamArrayList(i + 1, arg.get<const QoreListNode>(), xsink)) {
                    return -1;
                }
                break;
            case NT_STRING:
            case NT_NUMBER:
            case NT_DATE:
            case NT_INT:
            case NT_FLOAT:
            case NT_BOOLEAN:
            case NT_BINARY:
                if (bindParamArraySingleValue(i + 1, arg, xsink))
                    return -1;
                break;
            case NT_HASH: {
                const QoreHashNode* h = arg.get<const QoreHashNode>();
                QoreValue odbct = h->getKeyValue("^odbct^");
                QoreValue value = h->getKeyValue("^value^");
                if (!value || odbct.getType() != NT_INT) {
                    xsink->raiseException("ODBC-BIND-ERROR", "hash parameter not in correct format for odbc_bind; "
                        "the odbc module cannot bind hash values");
                    return -1;
                }
                if (bindParamArrayBindHash(i + 1, h, xsink))
                    return -1;
                break;
            }
            default:
                xsink->raiseException("ODBC-BIND-ERROR", "do not know how to bind values of type '%s'",
                    arg.getTypeName());
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
        handleStmtError("ODBC-COLUMN-METADATA-ERROR", "error occured when fetching result column count", xsink);
        return -1;
    }

    char name[512];
    name[0] = '\0';
    resColumns.resize(columns);
    for (int i = 0; i < columns; ++i) {
        ODBCResultColumn& col = resColumns[i];
        col.number = i + 1;
        SQLSMALLINT nameLength;
        ret = SQLDescribeColA(stmt, i + 1, reinterpret_cast<SQLCHAR*>(name), 512, &nameLength, &col.dataType,
            &col.colSize, &col.decimalDigits, &col.nullable);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching result column metadata with index #%d (column #%d)");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("ODBC-COLUMN-METADATA-ERROR", s.c_str(), i, i + 1);
            return -1;
        }

        ret = SQLColAttributeA(stmt, i + 1, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &col.byteSize);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("error occured when fetching result column metadata with index #%d (column #%d)");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("ODBC-COLUMN-METADATA-ERROR", s.c_str(), i, i + 1);
            return -1;
        }
        if (!conn->preserveCase()) {
            char* c = name;
            while (*c) {
                *c = ::tolower(*c);
                ++c;
            }
        }
        col.name = name;
    }
    return 0;
}

int ODBCStatement::bindParamArrayList(int column, const QoreListNode* lst, ExceptionSink* xsink) {
    size_t count = lst->size();
    bool absoluteDate;
    SQLLEN* indArray;
    SQLRETURN ret;

    // Find out datatype of values in the list.
    qore_type_t ntype = NT_NULL;
    for (size_t i = 0; i < count; i++) {
        QoreValue arg = lst->retrieveEntry(i);
        if (!arg.isNullOrNothing()) {
            if (ntype == NT_NULL) {
                ntype = arg.getType();
                if (ntype == NT_DATE)
                    absoluteDate = arg.get<const DateTimeNode>()->isAbsolute();
            }
            else if (ntype != arg.getType()) { // Different types in the same array -> error.
                xsink->raiseException("ODBC-BIND-ERROR",
                    "different datatypes in the same parameter array with index #%d (column #%d)", column-1, column);
                return -1;
            }
        }
    }

    switch (ntype) {
        case NT_STRING: {
            char* array;
            size_t maxlen;
            if (createArrayFromStringList(lst, array, indArray, maxlen, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                maxlen, 0, reinterpret_cast<SQLCHAR*>(array), maxlen, indArray);
            break;
        }
        case NT_NUMBER: {
            char* array;
            size_t maxlen;
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
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
                    getTimestampColsize(options), options.frPrec, array, sizeof(TIMESTAMP_STRUCT), indArray);
            } else {
                SQL_INTERVAL_STRUCT* array;
                if (createArrayFromRelativeDateList(lst, array, indArray, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                    SQL_INTERVAL_DAY_TO_SECOND,
                    getIntDaySecondColsize(options), options.frPrec, array, sizeof(SQL_INTERVAL_STRUCT), indArray);
            }
            break;
        }
        case NT_INT: {
            if (options.bigint == EBO_NATIVE) {
                int64* array;
                if (createArrayFromIntList(lst, array, indArray, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                    SQL_BIGINT, BIGINT_COLSIZE, 0, array, sizeof(int64), indArray);
            } else if (options.bigint == EBO_STRING) {
                char* array;
                size_t maxlen;
                if (createStrArrayFromIntList(lst, array, indArray, maxlen, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR,
                    SQL_CHAR, maxlen, 0, array, maxlen, indArray);
            }
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
            int8_t* array;
            if (createArrayFromBoolList(lst, array, indArray, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_STINYINT,
                SQL_TINYINT, 3, 0, array, sizeof(int8_t), 0);
            break;
        }
        case NT_BINARY: {
            void* array;
            size_t maxlen;
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
            xsink->raiseException("ODBC-BIND-ERROR", "unknown parameter datatype; this error should never happen...");
            return -1;
        }
    } // switch

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array with index #%d (column #%d)");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), column-1, column);
        return -1;
    }

    return 0;
}

int ODBCStatement::bindParamArraySingleValue(int column, QoreValue arg, ExceptionSink* xsink) {
    SQLLEN* indArray;
    SQLRETURN ret;

    if (arg.isNullOrNothing()) { // Bind NULL argument.
        char* array = arrayHolder.getNullArray(xsink);
        if (!array)
            return -1;
        indArray = arrayHolder.getNullIndArray(xsink);
        if (!indArray)
            return -1;

        ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 0, 0, array, 0, indArray);
        if (!SQL_SUCCEEDED(ret)) { // error
            std::string s("failed binding NULL single value parameter with index #%d (column #%d)");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), column-1, column);
            return -1;
        }
        return 0;
    }

    qore_type_t ntype = arg.getType();
    switch (ntype) {
        case NT_STRING: {
            size_t len;
            char* array;
            if (createArrayFromString(arg.get<const QoreStringNode>(), array, indArray, len, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR,
                len, 0, reinterpret_cast<SQLCHAR*>(array), len, indArray);
            break;
        }
        case NT_NUMBER: {
            size_t len;
            char* array;
            if (createArrayFromNumber(arg.get<const QoreNumberNode>(), array, indArray, len, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, array, len, indArray);
            break;
        }
        case NT_DATE: {
            const DateTimeNode* date = arg.get<const DateTimeNode>();
            if (date->isAbsolute()) {
                TIMESTAMP_STRUCT* array;
                if (createArrayFromAbsoluteDate(date, array, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
                    getTimestampColsize(options), options.frPrec, array, sizeof(TIMESTAMP_STRUCT), 0);
            } else {
                SQL_INTERVAL_STRUCT* array;
                if (createArrayFromRelativeDate(date, array, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND,
                    SQL_INTERVAL_DAY_TO_SECOND,
                    getIntDaySecondColsize(options), options.frPrec, array, sizeof(SQL_INTERVAL_STRUCT), 0);
            }
            break;
        }
        case NT_INT: {
            if (options.bigint == EBO_NATIVE) {
                int64* array;
                if (createArrayFromInt(arg.getAsBigInt(), array, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SBIGINT,
                    SQL_BIGINT, BIGINT_COLSIZE, 0, array, sizeof(int64), 0);
            } else if (options.bigint == EBO_STRING) {
                size_t len;
                char* array;
                if (createStrArrayFromInt(arg, array, indArray, len, xsink))
                    return -1;
                ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, len, 0, array, len, indArray);
            }
            break;
        }
        case NT_FLOAT: {
            double* array;
            if (createArrayFromFloat(arg.getAsFloat(), array, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                SQL_DOUBLE, DOUBLE_COLSIZE, 0, array, sizeof(double), 0);
            break;
        }
        case NT_BOOLEAN: {
            int8_t* array;
            if (createArrayFromBool(arg.getAsBool(), array, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_STINYINT,
                SQL_TINYINT, 3, 0, array, sizeof(int8_t), 0);
            break;
        }
        case NT_BINARY: {
            size_t len;
            void* array;
            if (createArrayFromBinary(arg.get<const BinaryNode>(), array, indArray, len, xsink))
                return -1;
            ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_BINARY,
                    SQL_BINARY, len, 0, array, len, indArray);
            break;
        }
        default: {
            assert(false);
            xsink->raiseException("ODBC-BIND-ERROR", "do not know how to bind values of type '%s'",
                arg.getFullTypeName());
            return -1;
        }
    } // switch

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array with index #%d (column #%d) of type '%s'");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), column-1, column, arg.getTypeName());
        return -1;
    }

    return 0;
}

int ODBCStatement::bindParamArrayBindHash(int column, const QoreHashNode* h, ExceptionSink* xsink) {
    SQLRETURN ret;
    QoreValue odbct = h->getKeyValue("^odbct^");
    QoreValue value = h->getKeyValue("^value^");
    int64 odbcType = odbct.getAsBigInt();
    switch (odbcType) {
        case SQL_C_SLONG:
            if (bindTypeSLongArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_ULONG:
            if (bindTypeULongArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_SSHORT:
            if (bindTypeSShortArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_USHORT:
            if (bindTypeUShortArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_STINYINT:
            if (bindTypeSTinyintArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_UTINYINT:
            if (bindTypeUTinyintArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_FLOAT:
            if (bindTypeFloatArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_TYPE_DATE:
            if (bindTypeDateArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_TYPE_TIME:
            if (bindTypeTimeArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_TYPE_TIMESTAMP:
            if (bindTypeTimestampArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_MONTH:
            if (bindTypeIntMonthArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_YEAR:
            if (bindTypeIntYearArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_YEAR_TO_MONTH:
            if (bindTypeIntMonthArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_DAY:
            if (bindTypeIntDayArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_HOUR:
            if (bindTypeIntHourArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_MINUTE:
            if (bindTypeIntMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_SECOND:
            if (bindTypeIntSecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_DAY_TO_HOUR:
            if (bindTypeIntDayHourArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_DAY_TO_MINUTE:
            if (bindTypeIntDayMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_DAY_TO_SECOND:
            if (bindTypeIntDaySecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_HOUR_TO_MINUTE:
            if (bindTypeIntHourMinuteArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_HOUR_TO_SECOND:
            if (bindTypeIntHourSecondArray(column, value, ret, xsink))
                return -1;
            break;
        case SQL_C_INTERVAL_MINUTE_TO_SECOND:
            if (bindTypeIntMinuteSecondArray(column, value, ret, xsink))
                return -1;
            break;
        default:
            xsink->raiseException("ODBC-BIND-ERROR", "odbc_bind used with an unknown type identifier: " QLLD,
                odbcType);
            return -1;
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("failed binding parameter array with index #%d (column #%d)");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("ODBC-BIND-ERROR", s.c_str(), column-1, column);
        return -1;
    }

    return 0;
}

int ODBCStatement::bindTypeSLong(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_SLONG odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < static_cast<int64>(LONG_MIN) || n > static_cast<int64>(LONG_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_SLONG odbc_bind",
            n);
        return -1;
    }

    int32_t* ival = paramHolder.addInt32(static_cast<int32_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SLONG,
        SQL_INTEGER, INTEGER_COLSIZE, 0, ival, sizeof(int32_t), 0);
    return 0;
}

int ODBCStatement::bindTypeULong(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_ULONG odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < 0 || n > static_cast<int64>(ULONG_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_ULONG odbc_bind",
            n);
        return -1;
    }

    uint32_t* ival = paramHolder.addUint32(static_cast<uint32_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_ULONG,
        SQL_INTEGER, INTEGER_COLSIZE, 0, ival, sizeof(uint32_t), 0);
    return 0;
}

int ODBCStatement::bindTypeSShort(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_SSHORT odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < static_cast<int64>(SHRT_MIN) || n > static_cast<int64>(SHRT_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_SSHORT "
            "odbc_bind", n);
        return -1;
    }

    int16_t* ival = paramHolder.addInt16(static_cast<int16_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SSHORT,
        SQL_SMALLINT, SMALLINT_COLSIZE, 0, ival, sizeof(int16_t), 0);
    return 0;
}

int ODBCStatement::bindTypeUShort(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_USHORT odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < 0 || n > static_cast<int64>(USHRT_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_USHORT "
            "odbc_bind", n);
        return -1;
    }

    uint16_t* ival = paramHolder.addUint16(static_cast<uint16_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_USHORT,
        SQL_SMALLINT, SMALLINT_COLSIZE, 0, ival, sizeof(uint16_t), 0);
    return 0;
}

int ODBCStatement::bindTypeSTinyint(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_STINYINT odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < static_cast<int64>(SCHAR_MIN) || n > static_cast<int64>(SCHAR_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_STINYINT "
            "odbc_bind", n);
        return -1;
    }

    int8_t* ival = paramHolder.addInt8(static_cast<int8_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_STINYINT,
        SQL_TINYINT, TINYINT_COLSIZE, 0, ival, sizeof(int8_t), 0);
    return 0;
}

int ODBCStatement::bindTypeUTinyint(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_INT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_UTINYINT odbc_bind");
        return -1;
    }
    int64 n = arg.getAsBigInt();
    if (n < 0 || n > static_cast<int64>(UCHAR_MAX)) {
        xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld does not fit the limits of ODBCT_UTINYINT "
            "odbc_bind", n);
        return -1;
    }

    uint8_t* ival = paramHolder.addUint8(static_cast<uint8_t>(n));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_UTINYINT,
        SQL_TINYINT, TINYINT_COLSIZE, 0, ival, sizeof(uint8_t), 0);
    return 0;
}

int ODBCStatement::bindTypeFloat(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_FLOAT) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value passed with ODBCT_FLOAT odbc_bind");
        return -1;
    }

    float* fval = paramHolder.addFloat(arg.getAsFloat());
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_FLOAT,
        SQL_REAL, REAL_COLSIZE, 0, fval, sizeof(float), 0);
    return 0;
}

int ODBCStatement::bindTypeDate(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_DATE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isRelative()) {
        xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_DATE odbc_bind");
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
        SQL_TYPE_DATE, DATE_COLSIZE, 0, dval, sizeof(DATE_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTime(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_TIME odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isRelative()) {
        xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_TIME odbc_bind");
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
        SQL_TYPE_TIME, TIME_COLSIZE, 0, tval, sizeof(TIME_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTimestamp(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_TIMESTAMP odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isRelative()) {
        xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_TIMESTAMP odbc_bind");
        return -1;
    }

    TIMESTAMP_STRUCT* tval = paramHolder.addTimestamp(getTimestampFromDate(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
        getTimestampColsize(options), options.frPrec, tval, sizeof(TIMESTAMP_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYear(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_YEAR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEAR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getYearInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR,
        SQL_INTERVAL_YEAR, INT_YEAR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMonth(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_MONTH odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MONTH odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMonthInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MONTH,
        SQL_INTERVAL_MONTH, INT_MONTH_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYearMonth(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_YEARMONTH odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEARMONTH odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getYearMonthInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR_TO_MONTH,
        SQL_INTERVAL_YEAR_TO_MONTH, INT_YEARMONTH_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDay(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_DAY odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAY odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY,
        SQL_INTERVAL_DAY, INT_DAY_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHour(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_HOUR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOUR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR,
        SQL_INTERVAL_HOUR, INT_HOUR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_MINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE,
        SQL_INTERVAL_MINUTE, INT_MINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_SECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_SECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_SECOND, SQL_INTERVAL_SECOND,
        getIntSecondColsize(options), options.frPrec, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayHour(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_DAYHOUR odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYHOUR odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayHourInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_HOUR,
        SQL_INTERVAL_DAY_TO_HOUR, INT_DAYHOUR_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_DAYMINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYMINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDayMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_MINUTE,
        SQL_INTERVAL_DAY_TO_MINUTE, INT_DAYMINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDaySecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_DAYSECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYSECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getDaySecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_DAY_TO_SECOND,
        getIntDaySecondColsize(options), options.frPrec, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourMinute(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_HOURMINUTE odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURMINUTE odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourMinuteInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_MINUTE,
        SQL_INTERVAL_HOUR_TO_MINUTE, INT_HOURMINUTE_COLSIZE, 0, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_HOURSECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURSECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getHourSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL_HOUR_TO_SECOND,
        getIntHourSecondColsize(options), options.frPrec, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinuteSecond(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t ntype = arg.getType();
    if (ntype != NT_DATE) {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value passed with ODBCT_INT_MINUTESECOND odbc_bind");
        return -1;
    }
    const DateTimeNode* date = arg.get<const DateTimeNode>();
    if (date->isAbsolute()) {
        xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTESECOND odbc_bind");
        return -1;
    }

    SQL_INTERVAL_STRUCT* ival = paramHolder.addInterval(getMinuteSecondInterval(date));
    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL_MINUTE_TO_SECOND,
        getIntMinuteSecondColsize(options), options.frPrec, ival, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeSLongArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    int32_t* array = arrayHolder.addInt32Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < static_cast<int64>(LONG_MIN) || n > static_cast<int64>(LONG_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                    "ODBCT_SLONG odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<int32_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < static_cast<int64>(LONG_MIN) || n > static_cast<int64>(LONG_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                "ODBCT_SLONG odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<int32_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_SLONG odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SLONG,
        SQL_INTEGER, INTEGER_COLSIZE, 0, array, sizeof(int32_t), 0);
    return 0;
}

int ODBCStatement::bindTypeULongArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    uint32_t* array = arrayHolder.addUint32Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < 0 || n > static_cast<int64>(ULONG_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                    "ODBCT_ULONG odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<uint32_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < 0 || n > static_cast<int64>(ULONG_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of ODBCT_ULONG "
                "odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<uint32_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_ULONG odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_ULONG,
        SQL_INTEGER, INTEGER_COLSIZE, 0, array, sizeof(uint32_t), 0);
    return 0;
}

int ODBCStatement::bindTypeSShortArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    int16_t* array = arrayHolder.addInt16Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < static_cast<int64>(SHRT_MIN) || n > static_cast<int64>(SHRT_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                    "ODBCT_SSHORT odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<int16_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < static_cast<int64>(SHRT_MIN) || n > static_cast<int64>(SHRT_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of ODBCT_SSHORT "
                "odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<int16_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_SSHORT odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_SSHORT,
        SQL_SMALLINT, SMALLINT_COLSIZE, 0, array, sizeof(int16_t), 0);
    return 0;
}

int ODBCStatement::bindTypeUShortArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    uint16_t* array = arrayHolder.addUint16Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < 0 || n > static_cast<int64>(USHRT_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value %ld " QLLD " does not fit the limits of "
                    "ODBCT_USHORT odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<uint16_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < 0 || n > static_cast<int64>(USHRT_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of ODBCT_USHORT "
                "odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<uint16_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_USHORT odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_USHORT,
        SQL_SMALLINT, SMALLINT_COLSIZE, 0, array, sizeof(uint16_t), 0);
    return 0;
}

int ODBCStatement::bindTypeSTinyintArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    int8_t* array = arrayHolder.addInt8Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < static_cast<int64>(SCHAR_MIN) || n > static_cast<int64>(SCHAR_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                    "ODBCT_STINYINT odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<int8_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < static_cast<int64>(SCHAR_MIN) || n > static_cast<int64>(SCHAR_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                "ODBCT_STINYINT odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<int8_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_STINYINT "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_STINYINT,
        SQL_TINYINT, TINYINT_COLSIZE, 0, array, sizeof(int8_t), 0);
    return 0;
}

int ODBCStatement::bindTypeUTinyintArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    uint8_t* array = arrayHolder.addUint8Array(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                    "required, %lu passed",
                column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            int64 n = lst->retrieveEntry(i).getAsBigInt();
            if (n < 0 || n > static_cast<int64>(UCHAR_MAX)) {
                xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                    "ODBCT_UTINYINT odbc_bind", n);
                return -1;
            }
            array[i] = static_cast<uint8_t>(n);
        }
    } else if (argtype == NT_INT) {
        int64 n = arg.getAsBigInt();
        if (n < 0 || n > static_cast<int64>(UCHAR_MAX)) {
            xsink->raiseException("ODBC-BIND-ERROR", "integer value " QLLD " does not fit the limits of "
                "ODBCT_UTINYINT odbc_bind", n);
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++)
            array[i] = static_cast<uint8_t>(n);
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-int value or non-int list passed with ODBCT_UTINYINT "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_UTINYINT,
        SQL_TINYINT, TINYINT_COLSIZE, 0, array, sizeof(uint8_t), 0);
    return 0;
}

int ODBCStatement::bindTypeFloatArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    float* array = arrayHolder.addFloatArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            float n = lst->retrieveEntry(i).getAsFloat();
            array[i] = n;
        }
    } else if (argtype == NT_FLOAT) {
        float n = arg.getAsFloat();
        for (size_t i = 0; i < arraySize; i++)
            array[i] = n;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-float value or non-float list passed with ODBCT_FLOAT "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_FLOAT,
        SQL_REAL, REAL_COLSIZE, 0, array, sizeof(float), 0);
    return 0;
}

int ODBCStatement::bindTypeDateArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    DATE_STRUCT* array = arrayHolder.addDateArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            DATE_STRUCT d;
            qore_tm info;
            info.clear();
            date->getInfo(date->getZone(), info);
            d.year = info.year;
            d.month = info.month;
            d.day = info.day;
            array[i] = d;
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isRelative()) {
            xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_DATE odbc_bind");
            return -1;
        }

        DATE_STRUCT d;
        qore_tm info;
        info.clear();
        date->getInfo(date->getZone(), info);
        d.year = info.year;
        d.month = info.month;
        d.day = info.day;
        for (size_t i = 0; i < arraySize; i++)
            array[i] = d;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_DATE odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
        SQL_TYPE_DATE, DATE_COLSIZE, 0, array, sizeof(DATE_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTimeArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    TIME_STRUCT* array = arrayHolder.addTimeArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            TIME_STRUCT t;
            qore_tm info;
            info.clear();
            date->getInfo(date->getZone(), info);
            t.hour = info.hour;
            t.minute = info.minute;
            t.second = info.second;
            array[i] = t;
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isRelative()) {
            xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_TIME odbc_bind");
            return -1;
        }

        TIME_STRUCT t;
        qore_tm info;
        info.clear();
        date->getInfo(date->getZone(), info);
        t.hour = info.hour;
        t.minute = info.minute;
        t.second = info.second;
        for (size_t i = 0; i < arraySize; i++)
            array[i] = t;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_TIME odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
        SQL_TYPE_TIME, TIME_COLSIZE, 0, array, sizeof(TIME_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeTimestampArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    TIMESTAMP_STRUCT* array = arrayHolder.addTimestampArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }
        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            TIMESTAMP_STRUCT t = getTimestampFromDate(date);
            array[i] = t;
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isRelative()) {
            xsink->raiseException("ODBC-BIND-ERROR", "a relative date value passed with ODBCT_TIMESTAMP odbc_bind");
            return -1;
        }

        TIMESTAMP_STRUCT t = getTimestampFromDate(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = t;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_TIMESTAMP "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP,
         getTimestampColsize(options), options.frPrec, array, sizeof(TIMESTAMP_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYearArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                    "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getYearInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEAR odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getYearInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_YEAR "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR,
        SQL_INTERVAL_YEAR, INT_YEAR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMonthArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getMonthInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MONTH odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getMonthInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MONTH "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MONTH,
        SQL_INTERVAL_MONTH, INT_MONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntYearMonthArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getYearMonthInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_YEARMONTH "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getYearMonthInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_YEARMONTH "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_YEAR_TO_MONTH,
        SQL_INTERVAL_YEAR_TO_MONTH, INT_YEARMONTH_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getDayInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAY odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getDayInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAY "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY,
        SQL_INTERVAL_DAY, INT_DAY_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getHourInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOUR odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getHourInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOUR "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR,
        SQL_INTERVAL_HOUR, INT_HOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getMinuteInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTE odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getMinuteInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MINUTE "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE,
        SQL_INTERVAL_MINUTE, INT_MINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getSecondInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_SECOND odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getSecondInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_SECOND "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_SECOND, SQL_INTERVAL_SECOND,
        getIntSecondColsize(options), options.frPrec, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayHourArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getDayHourInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYHOUR "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getDayHourInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYHOUR "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_HOUR,
        SQL_INTERVAL_DAY_TO_HOUR, INT_DAYHOUR_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDayMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getDayMinuteInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYMINUTE "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getDayMinuteInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYMINUTE "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_MINUTE,
        SQL_INTERVAL_DAY_TO_MINUTE, INT_DAYMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntDaySecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getDaySecondInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_DAYSECOND "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getDaySecondInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_DAYSECOND "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_DAY_TO_SECOND,
        getIntDaySecondColsize(options), options.frPrec, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourMinuteArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getHourMinuteInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURMINUTE "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getHourMinuteInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOURMINUTE "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_MINUTE,
        SQL_INTERVAL_HOUR_TO_MINUTE, INT_HOURMINUTE_COLSIZE, 0, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntHourSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getHourSecondInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_HOURSECOND "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getHourSecondInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_HOURSECOND "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL_HOUR_TO_SECOND,
        getIntHourSecondColsize(options), options.frPrec, array, sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::bindTypeIntMinuteSecondArray(int column, QoreValue arg, SQLRETURN& ret, ExceptionSink* xsink) {
    qore_type_t argtype = arg.getType();
    size_t arraySize = arrayHolder.getArraySize();
    SQL_INTERVAL_STRUCT* array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;

    if (argtype == NT_LIST) {
        const QoreListNode* lst = arg.get<const QoreListNode>();
        if (lst->size() != arraySize) {
            xsink->raiseException("ODBC-BIND-ERROR",
                "mismatch between the list size and required number of list elements, index #%d (column #%d); %lu "
                "required, %lu passed", column-1, column, arraySize, lst->size());
            return -1;
        }

        for (size_t i = 0; i < arraySize; i++) {
            const DateTimeNode* date = lst->retrieveEntry(i).get<const DateTimeNode>();
            array[i] = getMinuteSecondInterval(date);
        }
    } else if (argtype == NT_DATE) {
        const DateTimeNode* date = arg.get<const DateTimeNode>();
        if (date->isAbsolute()) {
            xsink->raiseException("ODBC-BIND-ERROR", "an absolute date value passed with ODBCT_INT_MINUTESECOND "
                "odbc_bind");
            return -1;
        }

        SQL_INTERVAL_STRUCT interval = getMinuteSecondInterval(date);
        for (size_t i = 0; i < arraySize; i++)
            array[i] = interval;
    } else {
        xsink->raiseException("ODBC-BIND-ERROR", "non-date value or non-date list passed with ODBCT_INT_MINUTESECOND "
            "odbc_bind");
        return -1;
    }

    ret = SQLBindParameter(stmt, column, SQL_PARAM_INPUT, SQL_C_INTERVAL_MINUTE_TO_SECOND,
        SQL_INTERVAL_MINUTE_TO_SECOND, getIntMinuteSecondColsize(options), options.frPrec, array,
        sizeof(SQL_INTERVAL_STRUCT), 0);
    return 0;
}

int ODBCStatement::createArrayFromStringList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, size_t& maxlen,
        ExceptionSink* xsink) {
    char** stringArray = arrayHolder.addCharArray(xsink);
    if (!stringArray)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue str = arg->retrieveEntry(i);
        if (str.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }

        size_t len = 0;
        stringArray[i] = getCharsFromString(str.get<const QoreStringNode>(), len, xsink);
        if (*xsink) {
            return -1;
        }
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }

    // We have to create one big array and put all the strings in it one after another.
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * maxlen);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            array[i*maxlen] = '\0';
            continue;
        }
        memcpy((array + i*maxlen), stringArray[i], indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromNumberList(const QoreListNode* arg, char*& array, SQLLEN*& indArray,
        size_t& maxlen, ExceptionSink* xsink) {
    char** stringArray = arrayHolder.addCharArray(xsink);
    if (!stringArray)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue n = arg->retrieveEntry(i);
        if (n.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }

        QoreStringValueHelper vh(n, QCS_USASCII, xsink);
        if (*xsink)
            return -1;
        size_t len = vh->strlen();
        stringArray[i] = vh.giveBuffer();
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }

    // We have to create one big array and put all the strings in it one after another.
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * maxlen);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            array[i*maxlen] = '\0';
            continue;
        }
        memcpy((array + i*maxlen), stringArray[i], indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromBinaryList(const QoreListNode* arg, void*& array, SQLLEN*& indArray, size_t& maxlen,
        ExceptionSink* xsink) {
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue bin = arg->retrieveEntry(i);
        if (bin.isNullOrNothing() || !bin.get<const BinaryNode>()->getPtr()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        indArray[i] = bin.get<const BinaryNode>()->size();
        maxlen = (maxlen >= static_cast<size_t>(indArray[i])) ? maxlen : indArray[i];
    }

    // We have to create one big array and put all the binaries in it one after another (very inefficient).
    char* charArray = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    array = static_cast<void*>(charArray);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * maxlen);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            charArray[i*maxlen] = '\0';
            continue;
        }
        const BinaryNode* bin = arg->retrieveEntry(i).get<const BinaryNode>();
        memcpy((charArray + i*maxlen), bin->getPtr(), indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromAbsoluteDateList(const QoreListNode* arg, TIMESTAMP_STRUCT*& array,
        SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addTimestampArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue date = arg->retrieveEntry(i);
        if (date.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = getTimestampFromDate(date.get<const DateTimeNode>());
        indArray[i] = sizeof(TIMESTAMP_STRUCT);
    }
    return 0;
}

int ODBCStatement::createArrayFromRelativeDateList(const QoreListNode* arg, SQL_INTERVAL_STRUCT*& array,
        SQLLEN*& indArray, ExceptionSink* xsink) {
    array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue date = arg->retrieveEntry(i);
        if (date.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = getIntervalFromDate(date.get<const DateTimeNode>());
        indArray[i] = sizeof(SQL_INTERVAL_STRUCT);
    }
    return 0;
}

int ODBCStatement::createArrayFromBoolList(const QoreListNode* arg, int8_t*& array, SQLLEN*& indArray,
        ExceptionSink* xsink) {
    array = arrayHolder.addInt8Array(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue bn = arg->retrieveEntry(i);
        if (bn.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = bn.getAsBool();
        indArray[i] = sizeof(int8_t);
    }
    return 0;
}

int ODBCStatement::createArrayFromIntList(const QoreListNode* arg, int64*& array, SQLLEN*& indArray,
        ExceptionSink* xsink) {
    array = arrayHolder.addInt64Array(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue in = arg->retrieveEntry(i);
        if (in.isNullOrNothing()) {
            array[i] = 0;
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = in.getAsBigInt();
        indArray[i] = sizeof(int64);
    }
    return 0;
}

int ODBCStatement::createStrArrayFromIntList(const QoreListNode* arg, char*& array, SQLLEN*& indArray, size_t& maxlen,
        ExceptionSink* xsink) {
    char** stringArray = arrayHolder.addCharArray(xsink);
    if (!stringArray)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    maxlen = 0;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue n = arg->retrieveEntry(i);
        if (n.isNullOrNothing()) {
            indArray[i] = SQL_NULL_DATA;
            continue;
        }

        QoreStringValueHelper vh(n, QCS_USASCII, xsink);
        if (*xsink)
            return -1;
        size_t len = vh->strlen();
        stringArray[i] = vh.giveBuffer();
        indArray[i] = len;
        if (len > maxlen)
            maxlen = len;
    }

    // We have to create one big array and put all the strings in it one after another.
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * maxlen]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * maxlen);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        if (indArray[i] == 0 || indArray[i] == SQL_NULL_DATA) {
            array[i*maxlen] = '\0';
            continue;
        }
        memcpy((array + i*maxlen), stringArray[i], indArray[i]);
    }
    return 0;
}

int ODBCStatement::createArrayFromFloatList(const QoreListNode* arg, double*& array, SQLLEN*& indArray,
        ExceptionSink* xsink) {
    array = arrayHolder.addDoubleArray(xsink);
    if (!array)
        return -1;
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;

    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++) {
        QoreValue fn = arg->retrieveEntry(i);
        if (fn.isNullOrNothing()) {
            array[i] = 0;
            indArray[i] = SQL_NULL_DATA;
            continue;
        }
        array[i] = fn.getAsFloat();
        indArray[i] = sizeof(double);
    }
    return 0;
}

int ODBCStatement::createArrayFromString(const QoreStringNode* arg, char*& array, SQLLEN*& indArray, size_t& len,
        ExceptionSink* xsink) {
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    char* val = paramHolder.addChars(getCharsFromString(arg, len, xsink));
    if (!val)
        return -1;
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * len);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        memcpy((array + i*len), val, len);
        indArray[i] = len;
    }

    return 0;
}

int ODBCStatement::createArrayFromNumber(const QoreNumberNode* arg, char*& array, SQLLEN*& indArray, size_t& len,
        ExceptionSink* xsink) {
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;
    QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
    if (*xsink)
        return -1;
    len = vh->strlen();
    size_t arraySize = arrayHolder.getArraySize();
    char* val = paramHolder.addChars(vh.giveBuffer());
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize * len);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        memcpy((array + i*len), val, len);
        indArray[i] = len;
    }

    return 0;
}

int ODBCStatement::createArrayFromBinary(const BinaryNode* arg, void*& array, SQLLEN*& indArray, size_t& len,
        ExceptionSink* xsink) {
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;
    len = arg->size();
    size_t arraySize = arrayHolder.getArraySize();
    void* val = const_cast<void*>(arg->getPtr());
    char* charArray = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!charArray) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %d bytes",
            arraySize * len);
        return -1;
    }
    array = static_cast<void*>(charArray);
    for (size_t i = 0; i < arraySize; i++) {
        memcpy((charArray + i*len), val, len);
        indArray[i] = len;
    }

    return 0;
}

int ODBCStatement::createArrayFromAbsoluteDate(const DateTimeNode* arg, TIMESTAMP_STRUCT*& array,
        ExceptionSink* xsink) {
    assert(arg->isAbsolute());
    TIMESTAMP_STRUCT val = getTimestampFromDate(arg);
    array = arrayHolder.addTimestampArray(xsink);
    if (!array)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; ++i) {
        array[i] = val;
    }
    return 0;
}

int ODBCStatement::createArrayFromRelativeDate(const DateTimeNode* arg, SQL_INTERVAL_STRUCT*& array,
        ExceptionSink* xsink) {
    assert(arg->isRelative());
    SQL_INTERVAL_STRUCT val = getIntervalFromDate(arg);
    array = arrayHolder.addIntervalArray(xsink);
    if (!array)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; ++i)
        array[i] = val;
    return 0;
}

int ODBCStatement::createArrayFromBool(bool val, int8_t*& array, ExceptionSink* xsink) {
    array = arrayHolder.addInt8Array(xsink);
    if (!array)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; ++i)
        array[i] = val;
    return 0;
}

int ODBCStatement::createArrayFromInt(int64 val, int64*& array, ExceptionSink* xsink) {
    array = arrayHolder.addInt64Array(xsink);
    if (!array)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; ++i)
        array[i] = val;
    return 0;
}

int ODBCStatement::createStrArrayFromInt(QoreValue arg, char*& array, SQLLEN*& indArray, size_t& len,
        ExceptionSink* xsink) {
    indArray = arrayHolder.addIndArray(xsink);
    if (!indArray)
        return -1;
    QoreStringValueHelper vh(arg, QCS_USASCII, xsink);
    if (*xsink)
        return -1;
    len = vh->strlen();
    size_t arraySize = arrayHolder.getArraySize();
    char* val = paramHolder.addChars(vh.giveBuffer());
    array = paramHolder.addChars(new (std::nothrow) char[arraySize * len]);
    if (!array) {
        xsink->raiseException("ODBC-MEMORY-ERROR", "could not allocate char array with size of %ld bytes",
            arraySize*len);
        return -1;
    }
    for (size_t i = 0; i < arraySize; i++) {
        memcpy((array + i*len), val, len);
        indArray[i] = len;
    }

    return 0;
}

int ODBCStatement::createArrayFromFloat(double val, double*& array, ExceptionSink* xsink) {
    array = arrayHolder.addDoubleArray(xsink);
    if (!array)
        return -1;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++)
        array[i] = val;
    return 0;
}

SQLLEN* ODBCStatement::createIndArray(SQLLEN indicator, ExceptionSink* xsink) {
    SQLLEN* array = arrayHolder.addIndArray(xsink);
    if (!array)
        return 0;
    size_t arraySize = arrayHolder.getArraySize();
    for (size_t i = 0; i < arraySize; i++)
        array[i] = indicator;
    return array;
}

QoreValue ODBCStatement::getColumnValue(int column, ODBCResultColumn& rcol, ExceptionSink* xsink) {
    SQLLEN indicator;
    SQLRETURN ret;

    /*
    fprintf(stderr, "getColumnValue: row=%d, col=%d, dataType=%d\n", readRows, column, rcol.dataType);
    fprintf(stderr, "col: number=%d, name='%s', colSize=%d, byteSize=%d, dDigits=%d\n",
        rcol.number, rcol.name.c_str(), rcol.colSize, rcol.byteSize, rcol.decimalDigits);
    */

    switch (rcol.dataType) {
        // Integer types.
        case SQL_INTEGER: {
            SQLBIGINT val;
            ret = SQLGetData(stmt, column, SQL_C_SBIGINT, &val, sizeof(SQLBIGINT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return val;
            }
            break;
        }
        case SQL_BIGINT: {
            SQLBIGINT val;
            ret = SQLGetData(stmt, column, SQL_C_SBIGINT, &val, sizeof(SQLBIGINT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return val;
            }
            break;
        }
        case SQL_SMALLINT: {
            SQLINTEGER val;
            ret = SQLGetData(stmt, column, SQL_C_SLONG, &val, sizeof(SQLINTEGER), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return static_cast<int64>(val);
            }
            break;
        }
        case SQL_TINYINT: {
            SQLSMALLINT val;
            ret = SQLGetData(stmt, column, SQL_C_SSHORT, &val, sizeof(SQLSMALLINT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return static_cast<int64>(val);
            }
            break;
        }

        // Float types.
        case SQL_FLOAT: {
            SQLDOUBLE val;
            ret = SQLGetData(stmt, column, SQL_C_DOUBLE, &val, sizeof(SQLDOUBLE), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return val;
            }
            break;
        }
        case SQL_DOUBLE: {
            SQLDOUBLE val;
            ret = SQLGetData(stmt, column, SQL_C_DOUBLE, &val, sizeof(SQLDOUBLE), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return val;
            }
            break;
        }
        case SQL_REAL: {
            SQLREAL val;
            ret = SQLGetData(stmt, column, SQL_C_FLOAT, &val, sizeof(SQLREAL), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return val;
            }
            break;
        }

        // Character types
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR: {
            // retrieve data with SQL_C_CHAR
            SQLWCHAR unused[1];
            ret = SQLGetData(stmt, column, SQL_C_CHAR, unused, 0, &indicator); // Find out data size
            if (ret == SQL_NO_DATA) {
                // No data, therefore returning empty string
                return new QoreStringNode;
            }
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SQLLEN buflen = indicator + 1; // Ending \0 char.
                std::unique_ptr<char> buf(new (std::nothrow) char[buflen]);
                if (!buf.get()) {
                    xsink->raiseException("DBI:ODBC:MEMORY-ERROR",
                        "could not allocate buffer for result character data of row #%d, index #%d (column #%d)",
                        readRows, column-1, column);
                    return QoreValue();
                }
                ret = SQLGetData(stmt, column, SQL_C_CHAR, reinterpret_cast<SQLPOINTER>(buf.get()), buflen,
                    &indicator);
                if (SQL_SUCCEEDED(ret)) {
                    // PostgreSQL-specific hack, needed because it returns BOOLEANs as VARCHAR values '0' & '1'
                    if (buflen >= 2 && !buf.get()[1] && (buf.get()[0] == '0' || buf.get()[0] == '1')) {
                        char descTypeName[32];
                        SQLColAttributeA(stmt, column, SQL_DESC_TYPE_NAME, descTypeName, 32, 0, 0);
                        if (strcmp(descTypeName, "bool") == 0) {
                            return (bool)((buf.get()[0]) - 48);
                        }
                    }

                    QoreStringNodeHolder rv(new QoreStringNode(buf.release(), indicator, buflen, getQoreEncoding()));
                    rv->trim_trailing(' ');
                    return rv.release();
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
                        "could not allocate buffer for result binary data of row #%d, index #%d (column #%d)",
                        readRows, column-1, column);
                    return QoreValue();
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
                if (options.numeric == ENO_OPTIMAL) {
                    char* dot = strchr(val, '.');
                    if (!dot) {
                        errno = 0;
                        long long num = strtoll(val, 0, 10);
                        if (errno == ERANGE)
                            return new QoreNumberNode(val);
                        return num;
                    }
                    SimpleRefHolder<QoreNumberNode> afterDot(new QoreNumberNode(dot+1));
                    if (afterDot->equals(0LL))
                        return strtoll(val, 0, 10);
                    return new QoreNumberNode(val);
                } else if (options.numeric == ENO_STRING) {
                    return new QoreStringNode(val, conn->getDatasource()->getQoreEncoding());
                } else {
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
                return (bool)val;
            }
            break;
        }

        // Time types.
        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP: {
            TIMESTAMP_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_TIMESTAMP, &val, sizeof(TIMESTAMP_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return DateTimeNode::makeAbsolute(serverTz, val.year, val.month, val.day, val.hour, val.minute,
                    val.second, val.fraction / 1000);
            }
            break;
        }
        case SQL_TIME:
        case SQL_TYPE_TIME: {
            TIME_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_TYPE_TIME, &val, sizeof(TIME_STRUCT), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                return DateTimeNode::makeRelative(0, 0, 0, val.hour, val.minute, val.second, 0);
            }
            break;
        }
        case SQL_DATE:
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
                    val.intval.day_second.fraction/1000, true));
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
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY_TO_MINUTE, &val, sizeof(SQL_INTERVAL_STRUCT),
                &indicator);
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
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_DAY_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT),
                &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_DAY_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, val.intval.day_second.day,
                    val.intval.day_second.hour,
                    val.intval.day_second.minute, val.intval.day_second.second, val.intval.day_second.fraction/1000,
                    true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_HOUR_TO_MINUTE: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_HOUR_TO_MINUTE, &val, sizeof(SQL_INTERVAL_STRUCT),
                &indicator);
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
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_HOUR_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT),
                &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_HOUR_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, val.intval.day_second.hour,
                    val.intval.day_second.minute,
                    val.intval.day_second.second, val.intval.day_second.fraction/1000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_INTERVAL_MINUTE_TO_SECOND: {
            SQL_INTERVAL_STRUCT val;
            ret = SQLGetData(stmt, column, SQL_C_INTERVAL_MINUTE_TO_SECOND, &val, sizeof(SQL_INTERVAL_STRUCT),
                &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                assert(val.interval_type == SQL_IS_MINUTE_TO_SECOND);
                SimpleRefHolder<DateTimeNode> d(new DateTimeNode(0, 0, 0, 0, val.intval.day_second.minute,
                    val.intval.day_second.second, val.intval.day_second.fraction/1000, true));
                if (val.interval_sign == SQL_TRUE)
                    d = d->unaryMinus();
                return d.release();
            }
            break;
        }
        case SQL_GUID: { // Same as UUID.
            SQLGUID val;
            ret = SQLGetData(stmt, column, SQL_C_GUID, &val, sizeof(SQLGUID), &indicator);
            if (SQL_SUCCEEDED(ret) && (indicator != SQL_NULL_DATA)) {
                SimpleRefHolder<QoreStringNode> s(new QoreStringNode);
                if (*s) {
                    s->sprintf("%x-%x-%x-%x-", val.Data1, val.Data2, val.Data3, val.Data4[0], val.Data4[1]);
                    s->sprintf("%x%x%x%x%x%x", val.Data4[2], val.Data4[3], val.Data4[4], val.Data4[5], val.Data4[6],
                        val.Data4[7]);
                }
                return s.release();
            }
            break;
        }
        default: {
            std::string s("do not know how to handle result value of type '%d'");
            ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
            xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.c_str(), rcol.dataType);
            return 0;
        }
    }

    if (!SQL_SUCCEEDED(ret)) { // error
        std::string s("error occured when getting value of row #%d, index #%d (column #%d)");
        ODBCErrorHelper::extractDiag(SQL_HANDLE_STMT, stmt, s);
        xsink->raiseException("DBI:ODBC:RESULT-ERROR", s.c_str(), readRows, column-1, column);
        return QoreValue();
    }

    if (indicator == SQL_NULL_DATA) {
        assert(rcol.nullable);
        return null();
    }

    assert(false);
    return QoreValue();
}

} // namespace odbc
