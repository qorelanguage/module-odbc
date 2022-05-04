/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
    ParamHolder.h

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

#ifndef _QORE_MODULE_ODBC_PARAMHOLDER_H
#define _QORE_MODULE_ODBC_PARAMHOLDER_H

#include <cstdint>
#include <vector>

#include <sql.h>
#include <sqlext.h>

#include "qore/common.h"

namespace odbc {

namespace intern {

//! Class used by @ref ODBCStatement for temporary storage of SQL parameters.
class ParamHolder {
public:
    DLLLOCAL ParamHolder() {
        strings.reserve(16);
        ints8.reserve(8);
        ints16.reserve(2);
        ints32.reserve(2);
        uints8.reserve(2);
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

    DLLLOCAL int8_t* addInt8(int8_t i) {
        ints8.push_back(i);
        return &(ints8[ints8.size()-1]);
    }

    DLLLOCAL int16_t* addInt16(int16_t i) {
        ints16.push_back(i);
        return &(ints16[ints16.size()-1]);
    }

    DLLLOCAL int32_t* addInt32(int32_t i) {
        ints32.push_back(i);
        return &(ints32[ints32.size()-1]);
    }

    DLLLOCAL uint8_t* addUint8(uint8_t i) {
        uints8.push_back(i);
        return &(uints8[uints8.size()-1]);
    }

    DLLLOCAL uint16_t* addUint16(uint16_t i) {
        uints16.push_back(i);
        return &(uints16[uints16.size()-1]);
    }

    DLLLOCAL uint32_t* addUint32(uint32_t i) {
        uints32.push_back(i);
        return &(uints32[uints32.size()-1]);
    }

    DLLLOCAL int64* addInt64(int64 i) {
        int64s.push_back(i);
        return &(int64s[int64s.size()-1]);
    }

    DLLLOCAL float* addFloat(float f) {
        floats.push_back(f);
        return &(floats[floats.size()-1]);
    }

    DLLLOCAL double* addDouble(double f) {
        doubles.push_back(f);
        return &(doubles[doubles.size()-1]);
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
        ints8.clear();
        ints16.clear();
        ints32.clear();
        uints8.clear();
        uints16.clear();
        uints32.clear();
        floats.clear();
        lengths.clear();
        dates.clear();
        times.clear();
    }

private:
    std::vector<char*> strings;
    std::vector<int8_t> ints8;
    std::vector<int16_t> ints16;
    std::vector<int32_t> ints32;
    std::vector<uint8_t> uints8;
    std::vector<uint16_t> uints16;
    std::vector<uint32_t> uints32;
    std::vector<int64> int64s;
    std::vector<float> floats;
    std::vector<double> doubles;
    std::vector<SQLLEN> lengths;
    std::vector<DATE_STRUCT> dates;
    std::vector<TIME_STRUCT> times;
    std::vector<TIMESTAMP_STRUCT> timestamps;
    std::vector<SQL_INTERVAL_STRUCT> intervals;
};

} // namespace intern

} // namespace odbc

#endif // _QORE_MODULE_ODBC_PARAMHOLDER_H

