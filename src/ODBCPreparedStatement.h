/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCPreparedStatement.h

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

#ifndef _QORE_MODULE_ODBC_ODBCPREPAREDSTATEMENT_H
#define _QORE_MODULE_ODBC_ODBCPREPAREDSTATEMENT_H

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

#include "ODBCStatement.h"

namespace odbc {

class ODBCConnection;

//! A class representing ODBC prepared statement.
class ODBCPreparedStatement : public ODBCStatement {
public:
    //! Constructor.
    DLLLOCAL ODBCPreparedStatement(ODBCConnection* c, ExceptionSink* xsink);

    //! Constructor.
    DLLLOCAL ODBCPreparedStatement(Datasource* ds, ExceptionSink* xsink);

    //! Destructor.
    DLLLOCAL ~ODBCPreparedStatement();

    //! Disabled copy constructor.
    DLLLOCAL ODBCPreparedStatement(const ODBCStatement& s) = delete;

    //! Disabled assignment operator.
    DLLLOCAL ODBCPreparedStatement& operator=(const ODBCStatement& s) = delete;

    //! Prepare an ODBC SQL statement.
    /** @param qstr Qore-style SQL statement
        @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int prepare(const QoreString& qstr, const QoreListNode* args, ExceptionSink* xsink);

    //! Execute the prepared statement.
    /** @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int exec(ExceptionSink* xsink);

    //! Bind the passed arguments to the statement.
    /** @param args SQL parameters
        @param xsink exception sink

        @return 0 for OK, -1 for error
     */
    DLLLOCAL int bind(const QoreListNode& args, ExceptionSink* xsink);

    //! Get one result row.
    /** @param xsink exception sink

        @return one result-set row
     */
    DLLLOCAL QoreHashNode* fetchRow(ExceptionSink* xsink);

    //! Get result rows (get result list).
    /** @param xsink exception sink
        @param rows maximum count of rows to return; if <= 0 the count of returned rows is not limited

        @return list of row hashes
     */
    DLLLOCAL QoreListNode* fetchRows(int maxRows, ExceptionSink* xsink);

    //! Get result columns (get result hash).
    /** @param xsink exception sink
        @param rows maximum count of rows to return; if <= 0 the count of returned rows is not limited

        @return hash of result column lists
     */
    DLLLOCAL QoreHashNode* fetchColumns(int maxRows, ExceptionSink* xsink);

    //! Retrieve the next result-set row.
    /** @param xsink exception sink

        @return true if a row was successfully retrieved, false if not (no more rows available) or an error occured
     */
    DLLLOCAL bool next(ExceptionSink* xsink);

private:
    //! Arguments bound to the statement.
    ReferenceHolder<QoreListNode> bindArgs;

    //! Output row prepared by calling next().
    ReferenceHolder<QoreHashNode> outputRow;
};

} // namespace odbc

#endif // _QORE_MODULE_ODBC_ODBCPREPAREDSTATEMENT_H

