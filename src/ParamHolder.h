/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ParamHolder.h

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

#ifndef _QORE_MODULE_ODBC_PARAMHOLDER_H
#define _QORE_MODULE_ODBC_PARAMHOLDER_H

#include <vector>

#include <sql.h>
#include <sqlext.h>

#include "qore/common.h"

// Needed because vector<bool> stores the values as bits instead of separate values in an array.
struct BoolWrapper {
    bool val;
    BoolWrapper(bool b) : val(b) {}
};

//! Class used by @ref ODBCStatement for temporary storage of SQL parameters.
class ParamHolder {
public:
    DLLLOCAL ParamHolder() {
        strings.reserve(16);
        bools.reserve(8);
        lengths.reserve(32);
        dates.reserve(4);
        timestamps.reserve(4);
        times.reserve(4);
        intervals.reserve(4);
    }
    DLLLOCAL ~ParamHolder() { clear(); }

    DLLLOCAL char* addChars(char* s) {
        strings.push_back(s);
        return s;
    }

    DLLLOCAL bool* addBool(bool b) {
        bools.push_back(b);
        return &(bools[bools.size()-1].val);
    }

    DLLLOCAL SQLLEN* addLength(SQLLEN l) {
        lengths.push_back(l);
        return &(lengths[lengths.size()-1]);
    }

    DLLLOCAL DATE_STRUCT* addDate(DATE_STRUCT d) {
        dates.push_back(d);
        return &(dates[dates.size()-1]);
    }

    DLLLOCAL TIME_STRUCT* addTime(TIME_STRUCT t) {
        times.push_back(t);
        return &(times[times.size()-1]);
    }

    DLLLOCAL TIMESTAMP_STRUCT* addTimestamp(TIMESTAMP_STRUCT ts) {
        timestamps.push_back(ts);
        return &(timestamps[timestamps.size()-1]);
    }

    DLLLOCAL SQL_INTERVAL_STRUCT* addInterval(SQL_INTERVAL_STRUCT i) {
        intervals.push_back(i);
        return &(intervals[intervals.size()-1]);
    }

    DLLLOCAL void clear() {
        unsigned int count = strings.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (strings[i]);

        strings.clear();
        bools.clear();
        lengths.clear();
        dates.clear();
        times.clear();
    }

private:
    std::vector<char*> strings;
    std::vector<BoolWrapper> bools;
    std::vector<SQLLEN> lengths;
    std::vector<DATE_STRUCT> dates;
    std::vector<TIME_STRUCT> times;
    std::vector<TIMESTAMP_STRUCT> timestamps;
    std::vector<SQL_INTERVAL_STRUCT> intervals;
};

#endif // _QORE_MODULE_ODBC_PARAMHOLDER_H

