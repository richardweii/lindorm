//
// Don't modify this file, the evaluation program is compiled
// based on this header file.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LINDORM_TSDB_CONTEST_CPP_COLUMNVALUE_H
#define LINDORM_TSDB_CONTEST_CPP_COLUMNVALUE_H

#include "Root.h"

namespace LindormContest {

    /**
    * The data in a column may be of one of the following three types: <br>
    *   1. string: no fixed length, stores a string
    *      (No character '\0' would be explicitly added to the tail.
    *      Do not use it as a c style char * string.
    *      Please pay attention to this, the valid length of the char *p
    *      is the string's size, i.e., if the string's length is 10, then
    *      you should only access p[0, 9], p[10] is illegal memory address)<br>
    *   2. integer: 4 bytes, supporting negative value<br>
    *   3. double float: 8 bytes, supporting negative value
    */
    enum ColumnType {
        COLUMN_TYPE_STRING = 1,
        COLUMN_TYPE_INTEGER = 2,
        COLUMN_TYPE_DOUBLE_FLOAT = 3,
        COLUMN_TYPE_UNINITIALIZED = 4
    };

    std::string getNameFromColumnType(ColumnType columnType);

    ColumnType getColumnTypeFromString(const std::string &str);

    /**
     * A column object relates to a data stored in this column for a specific row and a specific vin.
     * A column's data may be of type integer, double float or string. Each column has its columnFieldName for navigating.<br>
     * The memory structure of columnData:<br>
     *   1. If the type is integer, then the columnData is of size(int32_t) and can be fetched by {*(int32_t) columnData}.<br>
     *   2. If the type is double float, then the columnData is of size(double_t) and can be fetched by {*(double_t) columnData}.<br>
     *   3. If the type is string, then the first sizeof(int32_t) (4) bytes of the columnData is the size of the string which
     *      can be fetched by {*(int32_t) columnData}, e.g., the string size is 23, then {*(int32_t) columnData == 23}.
     *      The later bytes of the columnData is the string, you can fetch the string by {char *str = columnData + sizeof(int32_t)}.
     *      Pay attention that the string tail is not required to be ended with '\0',
     *      you must use the string conforming to its length property.<br>
     * <p>
     * To make it convenient, we have capsuled some methods for you to fetch the value of the column, if you want to
     * manipulate the structure of column by yourself, please pay attention to the instructions above. Pay attention
     * that the columnData should always be allocated by std::malloc since it would be reclaimed by std::free.
     * <p>
     * Usage Example: <br>
     * &nbsp;&nbsp;Column c1(15); // Create integer column. <br>
     * &nbsp;&nbsp;Column c2(12243.324); // Create double float column. <br>
     * &nbsp;&nbsp;Column c3(charPtr, 10); // Create string column. <br>
     * &nbsp;&nbsp;Column c = ...; <br>
     * &nbsp;&nbsp;ColumnType columnType = c.getColumnType(); <br>
     * &nbsp;&nbsp;if (columnType == COLUMN_TYPE_INTEGER) { <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;int32_t i; <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;c.getIntegerValue(i) <br>
     * &nbsp;&nbsp;} else if (columnType == COLUMN_TYPE_DOUBLE_FLOAT) { <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;double_t d; <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;c.getDoubleFloatValue(d); <br>
     * &nbsp;&nbsp;} else if (columnType == COLUMN_TYPE_STRING) { <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;std::pair&lt;int32_t, const char *	&gt; p(0, nullptr); <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;c.getStringValue(p); <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;int32_t stringLength = p.first; <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;char *charPtr = p.second; <br>
     * &nbsp;&nbsp;} <br>
     */
    typedef struct ColumnValue {
        ColumnType columnType;
        char *columnData;

        /**
         * Create with empty data. You may want to manipulate the inner
         * data structures by yourself, so that you can use this constructor.
         */
        explicit ColumnValue();

        /**
         * Create with integer type value.
         */
        explicit ColumnValue(int32_t value);

        /**
         * Create with double float type value.
         */
        explicit ColumnValue(double_t value);

        /**
         * Create with raw binary string value.
         */
        explicit ColumnValue(const char *valuePtr, int32_t valueLength);

        /**
         * Create with a string value capsuled by std::string.
         * The data at value[index, value.size()) would be copied and stored at this object.
         * Pay attention the '\0' at value.c_str[value.size()] is not copied.
         */
        explicit ColumnValue(const std::string &value);

        /**
         * Create with raw binary string value stored in vector object.
         * The value[0, value.size()) would be copied.
         */
        explicit ColumnValue(const std::vector<char> &value);

        ColumnValue(const ColumnValue &rhs);

        ColumnValue(ColumnValue &&rhs) noexcept;

        ColumnType getColumnType() const;

        /**
         * If this column is of integer type, upsert the value as the column's value.
         * @return 0 if the column is of integer type and the value written.
         */
        int getIntegerValue(int32_t &value) const;

        /**
         * If this column is of double float type, upsert the value as the column's value.
         * @return 0 if the column is of double float type and the value written.
         */
        int getDoubleFloatValue(double_t &value) const;

        /**
         * Write lengthStrPair with the first as its string's length, the second as the header pointer of the string.
         * The lifespan of the returned second string header pointer is the same with this object. You should not release the pointer.
         * The parameter lengthStrPair.second must be nullptr or this function would do nothing, this function won't overwrite a non-null pointer.
         * @return 0 if the column is of string type and the lengthStrPair written.
         *         -1 if the column's type is not string.
         *         -2 if the lengthStrPair.second is not nullptr.
         */
        int getStringValue(std::pair<int32_t, const char *> &lengthStrPair) const;

        /**
         * Return the data array size of this column.
         * For string column, the size is the string's size + sizeof(int32_t) (4) Bytes (storing the string's length).
         * For integer column, the size is sizeof(int32_t) (4) Bytes.
         * For double float column, the size is sizeof(double_t) (8) Bytes.
         */
        int32_t getRawDataSize() const;

        bool operator==(const ColumnValue &rhs) const;

        bool operator!=(const ColumnValue &rhs) const;

        ColumnValue& operator=(const ColumnValue &rhs);

        ~ColumnValue();
    }ColumnValue;

}

#endif //LINDORM_TSDB_CONTEST_CPP_COLUMNVALUE_H
