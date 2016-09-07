/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCResultColumn.h

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

#ifndef _QORE_ODBCRESULTCOLUMN_H
#define _QORE_ODBCRESULTCOLUMN_H

#include <string>

#include <sql.h>
#include <sqlext.h>

struct ODBCResultColumn {
public:
    //! Column number from 1 onwards.
    SQLSMALLINT number;

    //! Column name.
    std::string name;

    //! Column datatype;
    SQLSMALLINT dataType;

    //! Column size.
    SQLULEN colSize;

    //! Number of decimal digits of the column.
    SQLSMALLINT decimalDigits;

    //! Whether the column allows NULL values.
    SQLSMALLINT nullable;
};

#endif // _QORE_ODBCRESULTCOLUMN_H

