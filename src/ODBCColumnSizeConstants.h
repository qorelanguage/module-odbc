/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ODBCColumnSizeConstants.h

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

#ifndef _QORE_MODULE_ODBC_ODBCCOLUMNSIZECONSTANTS_H
#define _QORE_MODULE_ODBC_ODBCCOLUMNSIZECONSTANTS_H

#include <sql.h>
#include <sqlext.h>

#include "ODBCOptions.h"

/*

Interval Data Type Precision
-----------------------------

Precision for an interval data type includes interval leading precision,
interval precision, and seconds precision.

The leading field of an interval is a signed numeric. The maximum number of
digits for the leading field is determined by a quantity called interval leading
precision, which is a part of the data type declaration. For example, the
declaration: INTERVAL HOUR(5) TO MINUTE has an interval leading precision of 5;
the HOUR field can take values from –99999 through 99999. The interval leading
precision is contained in the SQL_DESC_DATETIME_INTERVAL_PRECISION field of the
descriptor record.

The list of fields that an interval data type is made up of is called interval
precision. It is not a numeric value, as the term "precision" might imply. For
example, the interval precision of the type INTERVAL DAY TO SECOND is the list
DAY, HOUR, MINUTE, SECOND. There is no descriptor field that holds this value;
the interval precision can always be determined by the interval data type.

Any interval data type that has a SECOND field has a seconds precision. This is
the number of decimal digits allowed in the fractional part of the seconds
value. This is different than for other data types, where precision indicates
the number of digits before the decimal point. The seconds precision of an
interval data type is the number of digits after the decimal point. For example,
if the seconds precision is set to 6, the number 123456 in the fraction field
would be interpreted as .123456 and the number 1230 would be interpreted
as .001230. For other data types, this is referred to as scale. Interval seconds
precision is contained in the SQL_DESC_PRECISION field of the descriptor. If the
precision of the fractional seconds component of the SQL interval value is
greater than what can be held in the C interval structure, it is driver-defined
whether the fractional seconds value in the SQL interval is rounded or truncated
when converted to the C interval structure.

When the SQL_DESC_CONCISE_TYPE field is set to an interval data type, the
SQL_DESC_TYPE field is set to SQL_INTERVAL and the
SQL_DESC_DATETIME_INTERVAL_CODE is set to the code for the interval data type.
The SQL_DESC_DATETIME_INTERVAL_PRECISION field is automatically set to the
default interval leading precision of 2, and the SQL_DESC_PRECISION field is
automatically set to the default interval seconds precision of 6. If either of
these values is not appropriate, the application should explicitly set the
descriptor field through a call to SQLSetDescField.

------------------------------------------------------------------------
source: https://msdn.microsoft.com/en-us/library/ms716251(v=vs.85).aspx

*/

namespace odbc {

//! ColumnSize value for the \c SQL_INTERVAL_YEAR datatype.
/** value = p, where p is the interval leading precision
    leading field (year) values: -32768 .. 32767 ==> 5
 */
DLLLOCAL const SQLULEN INT_YEAR_COLSIZE = 5;

//! ColumnSize value for the \c SQL_INTERVAL_MONTH datatype.
/** value = p, where p is the interval leading precision
    leading field (month) values: -2147483647 .. 2147483647 ==> 10
 */
DLLLOCAL const SQLULEN INT_MONTH_COLSIZE = 10;

//! ColumnSize value for the \c SQL_INTERVAL_YEAR_TO_MONTH datatype.
/** value = 3+p, where p is the interval leading precision
    leading field (year) values: -32768 .. 32767 ==> 3+5 ==> 8
 */
DLLLOCAL const SQLULEN INT_YEARMONTH_COLSIZE = 8;

//! ColumnSize value for the \c SQL_INTERVAL_DAY datatype.
/** value = p, where p is the interval leading precision
    leading field (day) values: -2147483647 .. 2147483647 ==> 10
 */
DLLLOCAL const SQLULEN INT_DAY_COLSIZE = 10;

//! ColumnSize value for the \c SQL_INTERVAL_HOUR datatype.
/** value = p, where p is the interval leading precision
    leading field (hour) values: -2147483647 .. 2147483647 ==> 10
 */
DLLLOCAL const SQLULEN INT_HOUR_COLSIZE = 10;

//! ColumnSize value for the \c SQL_INTERVAL_MINUTE datatype.
/** value = p, where p is the interval leading precision
    leading field (minute) values: -2147483647 .. 2147483647 ==> 10
 */
DLLLOCAL const SQLULEN INT_MINUTE_COLSIZE = 10;

//! Base ColumnSize value for the \c SQL_INTERVAL_SECOND datatype.
/** value = p (if s=0) or p+s+1 (if s>0), where p is the interval leading precision and s is the seconds precision
    leading field (second) values: -2147483647 .. 2147483647 ==> 10+s+1 ==> 11+s
 */
DLLLOCAL const SQLULEN INT_SECOND_COLSIZE_BASE = 11;

//! ColumnSize value for the \c SQL_INTERVAL_DAY_TO_HOUR datatype.
/** value = 3+p, where p is the interval leading precision
    leading field (day) values: -2147483647 .. 2147483647 ==> 3+10 ==> 13
 */
DLLLOCAL const SQLULEN INT_DAYHOUR_COLSIZE = 13;

//! ColumnSize value for the \c SQL_INTERVAL_DAY_TO_MINUTE datatype.
/** value = 6+p, where p is the interval leading precision
    leading field (day) values: -2147483647 .. 2147483647 ==> 6+10 ==> 16
 */
DLLLOCAL const SQLULEN INT_DAYMINUTE_COLSIZE = 16;

//! Base ColumnSize value for the \c SQL_INTERVAL_DAY_TO_SECOND datatype.
/** value = 9+p (if s=0) or 10+p+s (if s>0), where p is the interval leading precision and s is the seconds precision
    leading field (day) values: -2147483647 .. 2147483647 ==> 10+10+s ==> 20+s
 */
DLLLOCAL const SQLULEN INT_DAYSECOND_COLSIZE_BASE = 20;

//! ColumnSize value for the \c SQL_INTERVAL_HOUR_TO_MINUTE datatype.
/** value = 3+p, where p is the interval leading precision
    leading field (hour) values: -2147483647 .. 2147483647 ==> 3+10 ==> 13
 */
DLLLOCAL const SQLULEN INT_HOURMINUTE_COLSIZE = 13;

//! Base ColumnSize value for the \c SQL_INTERVAL_HOUR_TO_SECOND datatype.
/** value = 6+p (if s=0) or 7+p+s (if s>0), where p is the interval leading precision and s is the seconds precision
    leading field (hour) values: -2147483647 .. 2147483647 ==> 7+10+s ==> 17
 */
DLLLOCAL const SQLULEN INT_HOURSECOND_COLSIZE_BASE = 17;

//! Base ColumnSize value for the \c SQL_INTERVAL_MINUTE_TO_SECOND datatype.
/** value = 3+p (if s=0) or 4+p+s (if s>0), where p is the interval leading precision and s is the seconds precision
    leading field (minute) values: -2147483647 .. 2147483647 ==> 4+10+s ==> 14
 */
DLLLOCAL const SQLULEN INT_MINUTESECOND_COLSIZE_BASE = 14;

//! ColumnSize value for the \c SQL_TYPE_DATE datatype.
/** 10 (the number of characters in the yyyy-mm-dd format)
 */
DLLLOCAL const SQLULEN DATE_COLSIZE = 10;

//! ColumnSize value for the \c SQL_TYPE_TIME datatype.
/** 8 (the number of characters in the hh-mm-ss format),
    or 9 + s (the number of characters in the hh:mm:ss[.fff...] format, where s is the seconds precision)
    seconds precision here is 0 ==> 8
 */
DLLLOCAL const SQLULEN TIME_COLSIZE = 8;

//! Base ColumnSize value for the \c SQL_TYPE_TIMESTAMP datatype.
/** 16 (the number of characters in the yyyy-mm-dd hh:mm format),
    19 (the number of characters in the yyyy-mm-dd hh:mm:ss format)
    or 20 + s (the number of characters in the yyyy-mm-dd hh:mm:ss[.fff...] format, where s is the seconds precision)
    ==> 20+s
 */
DLLLOCAL const SQLULEN TIMESTAMP_COLSIZE_BASE = 20;

//! ColumnSize value for the \c SQL_BIGINT datatype.
/** 19 (if signed) or 20 (if unsigned)
    signed ==> 19
 */
DLLLOCAL const SQLULEN BIGINT_COLSIZE = 19;

//! ColumnSize value for the \c SQL_INTEGER datatype.
/** 10
 */
DLLLOCAL const SQLULEN INTEGER_COLSIZE = 10;

//! ColumnSize value for the \c SQL_SMALLINT datatype.
/** 5
 */
DLLLOCAL const SQLULEN SMALLINT_COLSIZE = 5;

//! ColumnSize value for the \c SQL_TINYINT datatype.
/** 5
 */
DLLLOCAL const SQLULEN TINYINT_COLSIZE = 3;

//! ColumnSize value for the \c SQL_REAL datatype.
/** 15
 */
DLLLOCAL const SQLULEN REAL_COLSIZE = 7;

//! ColumnSize value for the \c SQL_DOUBLE datatype.
/** 15
 */
DLLLOCAL const SQLULEN DOUBLE_COLSIZE = 15;


//! Get ColumnSize value for the \c SQL_INTERVAL_SECOND datatype.
/** @param opts ODBC connection options
    @return ColumnSize value
 */
DLLLOCAL SQLULEN getIntSecondColsize(ODBCOptions& opts) { return INT_SECOND_COLSIZE_BASE + opts.frPrec; }

//! Get ColumnSize value for the \c SQL_INTERVAL_DAY_TO_SECOND datatype.
/** @param opts ODBC connection options
    @return ColumnSize value
 */
DLLLOCAL SQLULEN getIntDaySecondColsize(ODBCOptions& opts) { return INT_DAYSECOND_COLSIZE_BASE + opts.frPrec; }

//! Get ColumnSize value for the \c SQL_INTERVAL_HOUR_TO_SECOND datatype.
/** @param opts ODBC connection options
    @return ColumnSize value
 */
DLLLOCAL SQLULEN getIntHourSecondColsize(ODBCOptions& opts) { return INT_HOURSECOND_COLSIZE_BASE + opts.frPrec; }

//! Get ColumnSize value for the \c SQL_INTERVAL_MINUTE_TO_SECOND datatype.
/** @param opts ODBC connection options
    @return ColumnSize value
 */
DLLLOCAL SQLULEN getIntMinuteSecondColsize(ODBCOptions& opts) { return INT_MINUTESECOND_COLSIZE_BASE + opts.frPrec; }

//! Get ColumnSize value for the \c SQL_TYPE_TIMESTAMP datatype.
/** @param opts ODBC connection options
    @return ColumnSize value
 */
DLLLOCAL SQLULEN getTimestampColsize(ODBCOptions& opts) { return TIMESTAMP_COLSIZE_BASE + opts.frPrec; }

} // namespace odbc

#endif // _QORE_MODULE_ODBC_ODBCCOLUMNSIZECONSTANTS_H

