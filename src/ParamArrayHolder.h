/* -*- mode: c++; indent-tabs-mode: nil -*- */
/*
  ParamArrayHolder.h

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

#ifndef _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H
#define _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H

#include <vector>

#include <sql.h>
#include <sqlext.h>

#include "qore/common.h"
#include "qore/ExceptionSink.h"

//! Class used by @ref ODBCStatement for temporary storage of SQL parameter arrays.
class ParamArrayHolder {
public:
    DLLLOCAL ParamArrayHolder() : nullArray(0), nullIndArray(0), arraySize(0) {
        chars.reserve(16);
        bools.reserve(8);
        ints.reserve(8);
        floats.reserve(8);
        timestamps.reserve(8);
        intervals.reserve(8);
        indicators.reserve(32);
    }
    DLLLOCAL ~ParamArrayHolder() { clear(); }

    DLLLOCAL void setArraySize(qore_size_t s) { arraySize = s; }
    DLLLOCAL qore_size_t getArraySize() const { return arraySize; }

    DLLLOCAL char** addCharArray(ExceptionSink* xsink) {
        chars.push_back(new char*[arraySize]);
        char** array = chars[chars.size()-1];
        if (!array) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate char array");
            return 0;
        }
        for (int i = 0; i < arraySize; i++)
            array[i] = NULL;
        return array;
    }

    DLLLOCAL bool* addBoolArray(ExceptionSink* xsink) {
        bools.push_back(new bool[arraySize]);
        bool* array = bools[bools.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate bool array");
        return array;
    }

    DLLLOCAL int64* addIntArray(ExceptionSink* xsink) {
        ints.push_back(new int64[arraySize]);
        int64* array = ints[ints.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate int64 array");
        return array;
    }

    DLLLOCAL double* addFloatArray(ExceptionSink* xsink) {
        floats.push_back(new double[arraySize]);
        double* array = floats[floats.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate double array");
        return array;
    }

    DLLLOCAL TIMESTAMP_STRUCT* addTimestampArray(ExceptionSink* xsink) {
        timestamps.push_back(new TIMESTAMP_STRUCT[arraySize]);
        TIMESTAMP_STRUCT* array = timestamps[timestamps.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate timestamp array");
        return array;
    }

    DLLLOCAL SQL_INTERVAL_STRUCT* addIntervalArray(ExceptionSink* xsink) {
        intervals.push_back(new SQL_INTERVAL_STRUCT[arraySize]);
        SQL_INTERVAL_STRUCT* array = intervals[intervals.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate interval array");
        return array;
    }

    DLLLOCAL SQLLEN* addIndArray(ExceptionSink* xsink) {
        indicators.push_back(new SQLLEN[arraySize]);
        SQLLEN* array = indicators[indicators.size()-1];
        if (!array)
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate indicator array");
        return array;
    }

    DLLLOCAL void** getNullArray(ExceptionSink* xsink) {
        if (nullArray)
            return nullArray;
        nullArray = new void*[arraySize];
        if (!nullArray) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate null array");
            return 0;
        }
        for (int i = 0; i < arraySize; i++)
            nullArray[i] = NULL;
        return nullArray;
    }

    DLLLOCAL SQLLEN* getNullIndArray(ExceptionSink* xsink) {
        if (nullIndArray)
            return nullIndArray;
        nullIndArray = new SQLLEN[arraySize];
        if (!nullIndArray) {
            xsink->raiseException("DBI:ODBC:MEMORY-ERROR", "could not allocate null indicator array");
            return 0;
        }
        for (int i = 0; i < arraySize; i++)
            nullIndArray[i] = SQL_NULL_DATA;
        return nullIndArray;
    }

    DLLLOCAL void clear() {
        unsigned int count = chars.size();
        for (unsigned int i = 0; i < count; i++) {
            for (unsigned int j = 0; j < arraySize; j++)
                delete [] (chars[i][j]);
            delete [] (chars[i]);
        }

        count = bools.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (bools[i]);

        count = ints.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (ints[i]);

        count = floats.size();
        for (unsigned int i = 0; i < count; i++)
            delete [] (floats[i]);

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
        bools.clear();
        ints.clear();
        floats.clear();
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
    std::vector<bool*> bools;
    std::vector<int64*> ints;
    std::vector<double*> floats;
    std::vector<TIMESTAMP_STRUCT*> timestamps;
    std::vector<SQL_INTERVAL_STRUCT*> intervals;
    std::vector<SQLLEN*> indicators;

    void** nullArray;
    SQLLEN* nullIndArray;

    qore_size_t arraySize;
};

#endif // _QORE_MODULE_ODBC_PARAMARRAYHOLDER_H

