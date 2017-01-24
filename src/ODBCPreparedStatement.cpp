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

#include "ODBCPreparedStatement.h"

#include <sql.h>
#include <sqlext.h>

namespace odbc {

ODBCPreparedStatement::ODBCPreparedStatement(ODBCConnection* c, ExceptionSink* xsink) :
    ODBCStatement(c, xsink),
    outputRow(xsink)
{
}

ODBCPreparedStatement::ODBCPreparedStatement(Datasource* ds, ExceptionSink* xsink) :
    ODBCStatement(ds, xsink),
    outputRow(xsink)
{
}

ODBCPreparedStatement::~ODBCPreparedStatement() {
}

int ODBCPreparedStatement::prepare(const QoreString& qstr, const QoreListNode* args, ExceptionSink* xsink) {
    command = qstr;

    // Convert string to required character encoding.
    std::unique_ptr<QoreString> str(qstr.convertEncoding(QCS_UTF8, xsink));
    if (!str.get())
        return -1;
    if (parse(str.get(), args, xsink))
        return -1;

    // Bind the arguments.
    if (args)
        bindArgs = args->listRefSelf();

#ifdef WORDS_BIGENDIAN
    TempEncodingHelper tstr(str.get(), QCS_UTF16BE, xsink);
#else
    TempEncodingHelper tstr(str.get(), QCS_UTF16LE, xsink);
#endif
    if (*xsink)
        return -1;

    SQLINTEGER textLen = getUTF8CharCount(const_cast<char*>(str->c_str()));
    SQLRETURN ret = SQLPrepareW(stmt, reinterpret_cast<SQLWCHAR*>(const_cast<char*>(tstr->getBuffer())), textLen);
    if (!SQL_SUCCEEDED(ret)) { // error
        handleStmtError("DBI:ODBC:PREPARE-ERROR", "error occured when preparing the SQL statement", xsink);
        return -1;
    }
    return 0;
}

int ODBCPreparedStatement::exec(ExceptionSink* xsink) {
    if (hasArrays(*bindArgs)) {
        if (bindInternArray(*bindArgs, xsink))
            return -1;
    }
    else {
        if (bindIntern(*bindArgs, xsink))
            return -1;
    }

    return execIntern(0, 0, xsink);
}

int ODBCPreparedStatement::bind(const QoreListNode& args, ExceptionSink* xsink) {
    bindArgs = args.listRefSelf();
    return 0;
}

QoreHashNode* ODBCPreparedStatement::fetchRow(ExceptionSink* xsink) {
    if (!outputRow) {
        xsink->raiseException("DBI:ODBC:FETCH-ROW-ERROR", "call SQLStatement::next() before calling SQLStatement::fetchRow()");
        return 0;
    }

    outputRow->refSelf();
    return *outputRow;
}

QoreListNode* ODBCPreparedStatement::fetchRows(int maxRows, ExceptionSink* xsink) {
    return getOutputList(xsink, maxRows);
}

QoreHashNode* ODBCPreparedStatement::fetchColumns(int maxRows, ExceptionSink* xsink) {
    return getOutputHash(xsink, true, maxRows);
}

bool ODBCPreparedStatement::next(ExceptionSink* xsink) {
    GetRowInternStatus status;
    outputRow = getRowIntern(status, xsink);
    if (status == EGRIS_OK)
        return true;
    return false;
}

int ODBCPreparedStatement::resetAfterLostConnection(ExceptionSink* xsink) {
    // We need an empty xsink for the preparing and binding functions.
    ExceptionSink xs;

    // Prepare the statement.
    if (prepare(command, *bindArgs, &xs)) {
        xsink->assimilate(xs);
        return -1;
    }

    // Re-bind parameters.
    if (hasArrays(*bindArgs)) {
        if (bindInternArray(*bindArgs, &xs)) {
            xsink->assimilate(xs);
            return -1;
        }
    }
    else {
        if (bindIntern(*bindArgs, &xs)) {
            xsink->assimilate(xs);
            return -1;
        }
    }

    return 0;
}

} // namespace odbc
