/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
    ParamArrayHolder.h

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

#ifndef _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H
#define _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H

#include <cstdint>
#include <vector>

#include <sql.h>
#include <sqlext.h>

#include "qore/common.h"
#include "qore/ExceptionSink.h"

namespace odbc {

namespace intern {

//! Class used by @ref ODBCStatement for temporary storage of SQL parameter arrays.
class ParamArrayHolder {
public:
    DLLLOCAL ParamArrayHolder() : nullArray(0), nullIndArray(0), arraySize(0) {
        chars.reserve(16);
        ints8.reserve(4);
        ints64.reserve(8);
        doubles.reserve(4);
        dates.reserve(4);
        times.reserve(4);
        timestamps.reserve(4);
        intervals.reserve(4);
        indicators.reserve(16);
    }
    DLLLOCAL ~ParamArrayHolder() { clear(); }

    DLLLOCAL void setArraySize(size_t s) { arraySize = s; }
    DLLLOCAL size_t getArraySize() const { return arraySize; }

    DLLLOCAL char** addCharArray(ExceptionSink* xsink) {
        chars.push_back(new (std::nothrow) char*[arraySize]);
        char** array = chars[chars.size()-1];
        if (!array) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array");
            return 0;
        }
        for (size_t i = 0; i < arraySize; i++)
            array[i] = NULL;
        return array;
    }

    DLLLOCAL int8_t* addInt8Array(ExceptionSink* xsink) {
        ints8.push_back(new (std::nothrow) int8_t[arraySize]);
        int8_t* array = ints8[ints8.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate int8_t (SQL_C_STINYINT) array");
        return array;
    }

    DLLLOCAL int16_t* addInt16Array(ExceptionSink* xsink) {
        ints16.push_back(new (std::nothrow) int16_t[arraySize]);
        int16_t* array = ints16[ints16.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate int16_t (SQL_C_SSHORT) array");
        return array;
    }

    DLLLOCAL int32_t* addInt32Array(ExceptionSink* xsink) {
        ints32.push_back(new (std::nothrow) int32_t[arraySize]);
        int32_t* array = ints32[ints32.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate int32_t (SQL_C_SLONG) array");
        return array;
    }

    DLLLOCAL int64* addInt64Array(ExceptionSink* xsink) {
        ints64.push_back(new (std::nothrow) int64[arraySize]);
        int64* array = ints64[ints64.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate int64 (SQL_C_SBIGINT) array");
        return array;
    }

    DLLLOCAL uint8_t* addUint8Array(ExceptionSink* xsink) {
        uints8.push_back(new (std::nothrow) uint8_t[arraySize]);
        uint8_t* array = uints8[uints8.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate uint8_t (SQL_C_UTINYINT) array");
        return array;
    }

    DLLLOCAL uint16_t* addUint16Array(ExceptionSink* xsink) {
        uints16.push_back(new (std::nothrow) uint16_t[arraySize]);
        uint16_t* array = uints16[uints16.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate uint16_t (SQL_C_USHORT) array");
        return array;
    }

    DLLLOCAL uint32_t* addUint32Array(ExceptionSink* xsink) {
        uints32.push_back(new (std::nothrow) uint32_t[arraySize]);
        uint32_t* array = uints32[uints32.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate uint32_t (SQL_C_ULONG) array");
        return array;
    }

    DLLLOCAL float* addFloatArray(ExceptionSink* xsink) {
        floats.push_back(new (std::nothrow) float[arraySize]);
        float* array = floats[floats.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate float (SQL_C_FLOAT) array");
        return array;
    }

    DLLLOCAL double* addDoubleArray(ExceptionSink* xsink) {
        doubles.push_back(new (std::nothrow) double[arraySize]);
        double* array = doubles[doubles.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate double (SQL_C_DOUBLE) array");
        return array;
    }

    DLLLOCAL DATE_STRUCT* addDateArray(ExceptionSink* xsink) {
        dates.push_back(new (std::nothrow) DATE_STRUCT[arraySize]);
        DATE_STRUCT* array = dates[dates.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate date (SQL_C_TYPE_DATE) array");
        return array;
    }

    DLLLOCAL TIME_STRUCT* addTimeArray(ExceptionSink* xsink) {
        times.push_back(new (std::nothrow) TIME_STRUCT[arraySize]);
        TIME_STRUCT* array = times[times.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate time (SQL_C_TYPE_TIME) array");
        return array;
    }

    DLLLOCAL TIMESTAMP_STRUCT* addTimestampArray(ExceptionSink* xsink) {
        timestamps.push_back(new (std::nothrow) TIMESTAMP_STRUCT[arraySize]);
        TIMESTAMP_STRUCT* array = timestamps[timestamps.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate timestamp (SQL_C_TYPE_TIMESTAMP) array");
        return array;
    }

    DLLLOCAL SQL_INTERVAL_STRUCT* addIntervalArray(ExceptionSink* xsink) {
        intervals.push_back(new (std::nothrow) SQL_INTERVAL_STRUCT[arraySize]);
        SQL_INTERVAL_STRUCT* array = intervals[intervals.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate interval (SQL_INTERVAL_STRUCT) array");
        return array;
    }

    DLLLOCAL SQLLEN* addIndArray(ExceptionSink* xsink) {
        indicators.push_back(new (std::nothrow) SQLLEN[arraySize]);
        SQLLEN* array = indicators[indicators.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate indicator (SQLLEN) array");
        return array;
    }

    DLLLOCAL char* getNullArray(ExceptionSink* xsink) {
        if (nullArray)
            return nullArray;
        nullArray = new (std::nothrow) char[arraySize];
        if (!nullArray) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate null array");
            return 0;
        }
        memset(nullArray, 0, arraySize);
        return nullArray;
    }

    DLLLOCAL SQLLEN* getNullIndArray(ExceptionSink* xsink) {
        if (nullIndArray)
            return nullIndArray;
        nullIndArray = new (std::nothrow) SQLLEN[arraySize];
        if (!nullIndArray) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate null indicator array");
            return 0;
        }
        for (size_t i = 0; i < arraySize; i++)
            nullIndArray[i] = SQL_NULL_DATA;
        return nullIndArray;
    }

    DLLLOCAL void clear() {
        unsigned int count = chars.size();
        for (unsigned int i = 0; i < count; i++) {
            for (size_t j = 0; j < arraySize; j++)
                delete [] (chars[i][j]);
            delete [] (chars[i]);
        }

        count = ints8.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (ints8[i]);

        count = ints16.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (ints16[i]);

        count = ints32.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (ints32[i]);

        count = ints64.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (ints64[i]);

        count = uints8.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (uints8[i]);

        count = uints16.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (uints16[i]);

        count = uints32.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (uints32[i]);

        count = floats.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (floats[i]);

        count = doubles.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (doubles[i]);

        count = dates.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (dates[i]);

        count = times.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (times[i]);

        count = timestamps.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (timestamps[i]);

        count = intervals.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (intervals[i]);

        count = indicators.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (indicators[i]);

        chars.clear();
        ints8.clear();
        ints16.clear();
        ints32.clear();
        ints64.clear();
        uints8.clear();
        uints16.clear();
        uints32.clear();
        floats.clear();
        doubles.clear();
        dates.clear();
        times.clear();
        timestamps.clear();
        intervals.clear();
        indicators.clear();

        if (nullArray) {
            delete [] nullArray;
            nullArray = 0;
        }
        if (nullIndArray) {
            delete [] nullIndArray;
            nullIndArray = 0;
        }
    }

private:
    std::vector<char**> chars;
    std::vector<int8_t*> ints8;
    std::vector<int16_t*> ints16;
    std::vector<int32_t*> ints32;
    std::vector<int64*> ints64;
    std::vector<uint8_t*> uints8;
    std::vector<uint16_t*> uints16;
    std::vector<uint32_t*> uints32;
    std::vector<float*> floats;
    std::vector<double*> doubles;
    std::vector<DATE_STRUCT*> dates;
    std::vector<TIME_STRUCT*> times;
    std::vector<TIMESTAMP_STRUCT*> timestamps;
    std::vector<SQL_INTERVAL_STRUCT*> intervals;
    std::vector<SQLLEN*> indicators;

    char* nullArray;
    SQLLEN* nullIndArray;

    size_t arraySize;
};

} // namespace intern

} // namespace odbc

#endif // _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H

