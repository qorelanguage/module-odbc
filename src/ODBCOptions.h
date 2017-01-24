/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCOptions.h

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

#ifndef _QORE_MODULE_ODBC_ODBCOPTIONS_H
#define _QORE_MODULE_ODBC_ODBCOPTIONS_H

#define OPT_BIGINT_NATIVE "bigint-native"    //!< BIGINT values bound as native BIGINT type
#define OPT_BIGINT_STRING "bigint-string"    //!< BIGINT values bound as strings
#define OPT_QORE_TIMEZONE "qore-timezone"    //!< timezone used for the connection

namespace odbc {

//! Option used for deciding how BIGINT parameters will be bound.
enum BigintOption {
    EBO_NATIVE = 0, // bind BIGINT parameters as native SQL_BIGINT
    EBO_STRING // bind BIGINT parameters as strings
};

//! Option used for deciding how NUMERIC results will be returned.
enum NumericOption {
    ENO_OPTIMAL = 0,
    ENO_STRING,
    ENO_NUMERIC // aka number
};

struct ODBCOptions {
public:
    ODBCOptions() : bigint(EBO_NATIVE), numeric(ENO_OPTIMAL) {}
    ODBCOptions(BigintOption bo, NumericOption no) : bigint(bo), numeric(no) {}

    //! Option used for deciding how BIGINT parameters will be bound.
    BigintOption bigint;

    //! Option used for deciding how NUMERIC results will be returned.
    NumericOption numeric;
};

} // namespace odbc

#endif // _QORE_MODULE_ODBC_ODBCOPTIONS_H

