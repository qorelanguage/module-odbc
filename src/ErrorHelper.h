/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ErrorHelper.h

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

#ifndef _QORE_ERRORHELPER_H
#define _QORE_ERRORHELPER_H

#include <sstream>
#include <string>

#include <sql.h>
#include <sqlext.h>

#include "qore/ExceptionSink.h"
#include "qore/QoreStringNode.h"
#include "qore/ReferenceHolder.h"

//! Error helper
class ErrorHelper {
private:
    DLLLOCAL ErrorHelper();
    DLLLOCAL ~ErrorHelper();
public:
    //! Raise an exception with description according to the passed format and arguments.
    static void exception(ExceptionSink* xsink, const char* err, const char* fmt, ...) {
        SimpleRefHolder<QoreStringNode> estr(new QoreStringNode);
        va_list args;
        while (fmt) {
            va_start(args, fmt);
            int rc = estr->vsprintf(fmt, args);
            va_end(args);
            if (!rc)
                break;
        }
        xsink->raiseException(err, estr->getBuffer());
    }

    //! Extract ODBC diagnostic records and output them to a stringstream.
    static void extractDiag(SQLSMALLINT handleType, SQLHANDLE& handle, std::stringstream& s) {
        SQLINTEGER i = 1;
        SQLINTEGER native;
        SQLCHAR state[7];
        SQLCHAR text[512];
        SQLSMALLINT len;
        SQLRETURN ret;

        while(true) {
            ret = SQLGetDiagRecA(handleType, handle, i++, state, &native, text, sizeof(text), &len);
            if (!SQL_SUCCEEDED(ret))
                break;
            s << "; " << state << " - " << native << ": " << text;
        }
    }

    static void extractState(SQLSMALLINT handleType, SQLHANDLE& handle, char* buf) {
        SQLINTEGER native;
        SQLSMALLINT textLen;
        SQLGetDiagRecA(handleType, handle, 1, reinterpret_cast<SQLCHAR*>(buf), &native, NULL, 0, &textLen);
    }
};

#endif // _QORE_ERRORHELPER_H

