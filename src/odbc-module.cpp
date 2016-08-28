/* indent-tabs-mode: nil -*- */
/*
  odbc-module.cpp

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

#include <sql.h>
#include <sqlext.h>

#include "qore/Qore.h"
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "ODBCConnection.h"

QoreStringNode *odbc_module_init();
void odbc_module_ns_init(QoreNamespace *rns, QoreNamespace *qns);
void odbc_module_delete();

// qore module symbols
DLLEXPORT char qore_module_name[] = "odbc";
DLLEXPORT char qore_module_version[] = PACKAGE_VERSION;
DLLEXPORT char qore_module_description[] = "ODBC database driver module";
DLLEXPORT char qore_module_author[] = "Ondrej Musil";
DLLEXPORT char qore_module_url[] = "http://qore.org";
DLLEXPORT int qore_module_api_major = QORE_MODULE_API_MAJOR;
DLLEXPORT int qore_module_api_minor = QORE_MODULE_API_MINOR;
DLLEXPORT qore_module_init_t qore_module_init = odbc_module_init;
DLLEXPORT qore_module_ns_init_t qore_module_ns_init = odbc_module_ns_init;
DLLEXPORT qore_module_delete_t qore_module_delete = odbc_module_delete;
#ifdef _QORE_HAS_QL_MIT
DLLEXPORT qore_license_t qore_module_license = QL_MIT;
#else
DLLEXPORT qore_license_t qore_module_license = QL_LGPL;
#endif
DLLEXPORT char qore_module_license_str[] = "MIT";

static DBIDriver* DBID_ODBC;

// capabilities of this driver
int DBI_ODBC_CAPS =
    DBI_CAP_TRANSACTION_MANAGEMENT
    | DBI_CAP_CHARSET_SUPPORT
    | DBI_CAP_LOB_SUPPORT
    | DBI_CAP_STORED_PROCEDURES
    | DBI_CAP_BIND_BY_VALUE
    | DBI_CAP_BIND_BY_PLACEHOLDER
    | DBI_CAP_HAS_ARRAY_BIND
    | DBI_CAP_HAS_NUMBER_SUPPORT
    | DBI_CAP_AUTORECONNECT
#ifdef _QORE_HAS_DBI_EXECRAW
    | DBI_CAP_HAS_EXECRAW
#endif
#ifdef _QORE_HAS_DBI_EXECRAW
    | DBI_CAP_HAS_EXECRAW
#endif
#ifdef _QORE_HAS_TIME_ZONES
    | DBI_CAP_TIME_ZONE_SUPPORT
#endif
#ifdef _QORE_HAS_FIND_CREATE_TIMEZONE
    | DBI_CAP_SERVER_TIME_ZONE
#endif
    ;


static int odbc_open(Datasource* ds, ExceptionSink* xsink) {
    //ODBCConnection* conn = new ODBCConnection(ds, lstr.getBuffer(), xsink);
    ODBCConnection* conn = new ODBCConnection(ds, "", xsink);

    if (*xsink) {
        delete conn;
        return -1;
    }

    ds->setPrivateData((void*)conn);
    return 0;
}

static int odbc_close(Datasource* ds) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    ds->setPrivateData(NULL);
    delete conn;
    return 0;
}

static AbstractQoreNode* odbc_select(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->exec(qstr, args, xsink);
}

#ifdef _QORE_HAS_DBI_SELECT_ROW
static AbstractQoreNode* odbc_select_row(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->selectRow(qstr, args, xsink);
}
#endif

static AbstractQoreNode* odbc_select_rows(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->selectRows(qstr, args, xsink);
}

static AbstractQoreNode* odbc_exec(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->exec(qstr, args, xsink);
}

#ifdef _QORE_HAS_DBI_EXECRAW
static AbstractQoreNode* odbc_execRaw(Datasource* ds, const QoreString* qstr, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->execRaw(qstr, xsink);
}
#endif

static int odbc_begin_transaction(Datasource* ds, ExceptionSink* xsink) {
    return 0;
}

static int odbc_commit(Datasource* ds, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->commit(xsink);
}

static int odbc_rollback(Datasource* ds, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return conn->rollback(xsink);
}

static AbstractQoreNode* odbc_get_client_version(const Datasource* ds, ExceptionSink* xsink) {
    // TODO
    return 0;
}

static AbstractQoreNode* odbc_get_server_version(Datasource* ds, ExceptionSink* xsink) {
    ODBCConnection* conn = static_cast<ODBCConnection *>(ds->getPrivateData());
    return new QoreBigIntNode(conn->getServerVersion());
}


QoreNamespace OdbcNS("odbc");

QoreStringNode *odbc_module_init() {

    qore_dbi_method_list methods;
    methods.add(QDBI_METHOD_OPEN, odbc_open);
    methods.add(QDBI_METHOD_CLOSE, odbc_close);
    methods.add(QDBI_METHOD_SELECT, odbc_exec);
#ifdef _QORE_HAS_DBI_SELECT_ROW
    methods.add(QDBI_METHOD_SELECT_ROW, odbc_select_row);
#endif
    methods.add(QDBI_METHOD_SELECT_ROWS, odbc_select_rows);
    methods.add(QDBI_METHOD_EXEC, odbc_exec);
#ifdef _QORE_HAS_DBI_EXECRAW
    methods.add(QDBI_METHOD_EXECRAW, odbc_execRaw);
#endif
    methods.add(QDBI_METHOD_COMMIT, odbc_commit);
    methods.add(QDBI_METHOD_ROLLBACK, odbc_rollback);
    methods.add(QDBI_METHOD_BEGIN_TRANSACTION, odbc_begin_transaction);
    methods.add(QDBI_METHOD_ABORT_TRANSACTION_START, odbc_rollback);
    methods.add(QDBI_METHOD_GET_SERVER_VERSION, odbc_get_server_version);
    methods.add(QDBI_METHOD_GET_CLIENT_VERSION, odbc_get_client_version);

    // prepared statement stuff

    DBID_ODBC = DBI.registerDriver("odbc", methods, DBI_ODBC_CAPS);

    return 0;
}

void odbc_module_ns_init(QoreNamespace *rns, QoreNamespace *qns) {
    qns->addNamespace(OdbcNS.copy());
}

void odbc_module_delete() {
    // nothing to do here in this case
}

