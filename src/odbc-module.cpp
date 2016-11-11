/* indent-tabs-mode: nil -*- */
/*
  odbc-module.cpp

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

#include <sql.h>
#include <sqlext.h>

#include "qore/Qore.h"
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "ODBCConnection.h"
#include "ODBCPreparedStatement.h"

void init_odbc_functions(QoreNamespace& ns);
void init_odbc_constants(QoreNamespace& ns);

QoreStringNode *odbc_module_init();
void odbc_module_ns_init(QoreNamespace *rns, QoreNamespace *qns);
void odbc_module_delete();

// qore module symbols
DLLEXPORT char qore_module_name[] = "odbc";
DLLEXPORT char qore_module_version[] = PACKAGE_VERSION;
DLLEXPORT char qore_module_description[] = "ODBC database driver module";
DLLEXPORT char qore_module_author[] = "Ondrej Musil <ondrej.musil@qoretechnologies.com>";
DLLEXPORT char qore_module_url[] = "http://qore.org";
DLLEXPORT int qore_module_api_major = QORE_MODULE_API_MAJOR;
DLLEXPORT int qore_module_api_minor = QORE_MODULE_API_MINOR;
DLLEXPORT qore_module_init_t qore_module_init = odbc_module_init;
DLLEXPORT qore_module_ns_init_t qore_module_ns_init = odbc_module_ns_init;
DLLEXPORT qore_module_delete_t qore_module_delete = odbc_module_delete;

DLLEXPORT qore_license_t qore_module_license = QL_MIT;
DLLEXPORT char qore_module_license_str[] = "MIT";

static DBIDriver* DBID_ODBC;

// capabilities of this driver
int DBI_ODBC_CAPS =
    DBI_CAP_TRANSACTION_MANAGEMENT
    | DBI_CAP_CHARSET_SUPPORT
    | DBI_CAP_LOB_SUPPORT
    | DBI_CAP_STORED_PROCEDURES
    | DBI_CAP_BIND_BY_VALUE
    | DBI_CAP_HAS_ARRAY_BIND
    | DBI_CAP_HAS_NUMBER_SUPPORT
    | DBI_CAP_HAS_OPTION_SUPPORT
    | DBI_CAP_HAS_SELECT_ROW
    | DBI_CAP_HAS_STATEMENT
    | DBI_CAP_HAS_DESCRIBE
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
    odbc::ODBCConnection* conn = new odbc::ODBCConnection(ds, xsink);
    if (*xsink) {
        delete conn;
        return -1;
    }

    ds->setPrivateData((void*)conn);
    return 0;
}

static int odbc_close(Datasource* ds) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    ds->setPrivateData(0);
    delete conn;
    return 0;
}

static AbstractQoreNode* odbc_select(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return conn->exec(qstr, args, xsink);
}

#ifdef _QORE_HAS_DBI_SELECT_ROW
static QoreHashNode* odbc_select_row(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return conn->selectRow(qstr, args, xsink);
}
#endif

static AbstractQoreNode* odbc_select_rows(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return conn->selectRows(qstr, args, xsink);
}

static AbstractQoreNode* odbc_exec(Datasource* ds, const QoreString* qstr, const QoreListNode* args, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return conn->exec(qstr, args, xsink);
}

#ifdef _QORE_HAS_DBI_EXECRAW
static AbstractQoreNode* odbc_execRaw(Datasource* ds, const QoreString* qstr, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return conn->execRaw(qstr, xsink);
}
#endif

static int odbc_begin_transaction(Datasource* ds, ExceptionSink* xsink) {
    return 0;
}

static int odbc_commit(Datasource* ds, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return -1;
    }
    return conn->commit(xsink);
}

static int odbc_rollback(Datasource* ds, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return -1;
    }
    return conn->rollback(xsink);
}

static AbstractQoreNode* odbc_get_client_version(const Datasource* ds, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return new QoreBigIntNode(conn->getClientVersion());
}

static AbstractQoreNode* odbc_get_server_version(Datasource* ds, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = static_cast<odbc::ODBCConnection *>(ds->getPrivateData());
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return 0;
    }
    return new QoreBigIntNode(conn->getServerVersion());
}

static int odbc_stmt_prepare(SQLStatement* stmt, const QoreString& str, const QoreListNode* args, ExceptionSink* xsink) {
    assert(!stmt->getPrivateData());

    odbc::ODBCPreparedStatement* ps = new odbc::ODBCPreparedStatement(stmt->getDatasource(), xsink);
    if (*xsink) {
        delete ps;
        return -1;
    }
    stmt->setPrivateData(ps);

    return ps->prepare(str, args, xsink);
}

static int odbc_stmt_prepare_raw(SQLStatement* stmt, const QoreString& str, ExceptionSink* xsink) {
    assert(!stmt->getPrivateData());

    odbc::ODBCPreparedStatement* ps = new odbc::ODBCPreparedStatement(stmt->getDatasource(), xsink);
    if (*xsink) {
        delete ps;
        return -1;
    }
    stmt->setPrivateData(ps);

    return ps->prepare(str, 0, xsink);
}

static int odbc_stmt_bind(SQLStatement* stmt, const QoreListNode& args, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->bind(args, xsink);
}

static int odbc_stmt_bind_placeholders(SQLStatement* stmt, const QoreListNode& args, ExceptionSink* xsink) {
    xsink->raiseException("DBI:ODBC:BIND-PLACEHHODERS-ERROR", "binding placeholders is not necessary or supported with the odbc driver");
    return -1;
}

static int odbc_stmt_bind_values(SQLStatement* stmt, const QoreListNode& args, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->bind(args, xsink);
}

static int odbc_stmt_exec(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->exec(xsink);
}

static int odbc_stmt_define(SQLStatement* stmt, ExceptionSink* xsink) {
    // define is a noop in the odbc driver
    return 0;
}

static int odbc_stmt_affected_rows(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->rowsAffected();
}

static QoreHashNode* odbc_stmt_get_output(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->getOutputHash(xsink, false);
}

static QoreHashNode* odbc_stmt_get_output_rows(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->getOutputHash(xsink, false);
}

static QoreHashNode* odbc_stmt_fetch_row(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->fetchRow(xsink);
}

static QoreListNode* odbc_stmt_fetch_rows(SQLStatement* stmt, int maxRows, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->fetchRows(maxRows, xsink);
}

static QoreHashNode* odbc_stmt_fetch_columns(SQLStatement* stmt, int maxRows, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->fetchColumns(maxRows, xsink);
}

static QoreHashNode* odbc_stmt_describe(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->describe(xsink);
}

static bool odbc_stmt_next(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    return ps->next(xsink);
}

static int odbc_stmt_close(SQLStatement* stmt, ExceptionSink* xsink) {
    odbc::ODBCPreparedStatement* ps = static_cast<odbc::ODBCPreparedStatement*>(stmt->getPrivateData());
    assert(ps);

    delete ps;
    stmt->setPrivateData(0);
    return *xsink ? -1 : 0;
}

static int odbc_opt_set(Datasource* ds, const char* opt, const AbstractQoreNode* val, ExceptionSink* xsink) {
    odbc::ODBCConnection* conn = (odbc::ODBCConnection*)ds->getPrivateData();
    if (!conn) {
        xsink->raiseException("DBI:ODBC:NO-CONNECTION-ERROR", "there is no open connection");
        return -1;
    }
    return conn->setOption(opt, val, xsink);
}

static AbstractQoreNode* odbc_opt_get(const Datasource* ds, const char* opt) {
    odbc::ODBCConnection* conn = (odbc::ODBCConnection*)ds->getPrivateData();
    if (!conn) {
        return 0;
    }
    return conn->getOption(opt);
}

QoreNamespace OdbcNS("odbc");

QoreStringNode *odbc_module_init() {
    init_odbc_functions(OdbcNS);
    init_odbc_constants(OdbcNS);

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
    methods.add(QDBI_METHOD_GET_CLIENT_VERSION, odbc_get_client_version);
    methods.add(QDBI_METHOD_GET_SERVER_VERSION, odbc_get_server_version);

    methods.add(QDBI_METHOD_STMT_PREPARE, odbc_stmt_prepare);
    methods.add(QDBI_METHOD_STMT_PREPARE_RAW, odbc_stmt_prepare_raw);
    methods.add(QDBI_METHOD_STMT_BIND, odbc_stmt_bind);
    methods.add(QDBI_METHOD_STMT_BIND_PLACEHOLDERS, odbc_stmt_bind_placeholders);
    methods.add(QDBI_METHOD_STMT_BIND_VALUES, odbc_stmt_bind_values);
    methods.add(QDBI_METHOD_STMT_EXEC, odbc_stmt_exec);
    methods.add(QDBI_METHOD_STMT_DEFINE, odbc_stmt_define);
    methods.add(QDBI_METHOD_STMT_FETCH_ROW, odbc_stmt_fetch_row);
    methods.add(QDBI_METHOD_STMT_FETCH_ROWS, odbc_stmt_fetch_rows);
    methods.add(QDBI_METHOD_STMT_FETCH_COLUMNS, odbc_stmt_fetch_columns);
    methods.add(QDBI_METHOD_STMT_DESCRIBE, odbc_stmt_describe);
    methods.add(QDBI_METHOD_STMT_NEXT, odbc_stmt_next);
    methods.add(QDBI_METHOD_STMT_CLOSE, odbc_stmt_close);
    methods.add(QDBI_METHOD_STMT_AFFECTED_ROWS, odbc_stmt_affected_rows);
    methods.add(QDBI_METHOD_STMT_GET_OUTPUT, odbc_stmt_get_output);
    methods.add(QDBI_METHOD_STMT_GET_OUTPUT_ROWS, odbc_stmt_get_output_rows);

    methods.add(QDBI_METHOD_OPT_SET, odbc_opt_set);
    methods.add(QDBI_METHOD_OPT_GET, odbc_opt_get);

    methods.registerOption(DBI_OPT_NUMBER_OPT, "when set, numeric/decimal values are returned as integers if possible, otherwise as arbitrary-precision number values; the argument is ignored; setting this option turns it on and turns off 'string-numbers' and 'numeric-numbers'");
    methods.registerOption(DBI_OPT_NUMBER_STRING, "when set, numeric/decimal values are returned as strings for backwards-compatibility; the argument is ignored; setting this option turns it on and turns off 'optimal-numbers' and 'numeric-numbers'");
    methods.registerOption(DBI_OPT_NUMBER_NUMERIC, "when set, numeric/decimal values are returned as arbitrary-precision number values; the argument is ignored; setting this option turns it on and turns off 'string-numbers' and 'optimal-numbers'");
    methods.registerOption("qore-timezone", "set the server-side timezone, value must be a string in the format accepted by Timezone::constructor() on the client (ie either a region name or a UTC offset like \"+01:00\"), if not set the server's time zone will be assumed to be the same as the client's", stringTypeInfo);

    DBID_ODBC = DBI.registerDriver("odbc", methods, DBI_ODBC_CAPS);

    return 0;
}

void odbc_module_ns_init(QoreNamespace *rns, QoreNamespace *qns) {
    qns->addNamespace(OdbcNS.copy());
}

void odbc_module_delete() {
    // nothing to do here in this case
}

