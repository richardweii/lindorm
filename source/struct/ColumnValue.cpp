//
// Don't modify this file, the evaluation program is compiled
// based on this cpp file.
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

#include "struct/ColumnValue.h"

namespace LindormContest {

    static void release(ColumnValue &column) {
        if (column.columnData != nullptr) {
            std::free(column.columnData);
            column.columnData = nullptr;
        }
    }

    static void* malloc(size_t size) {
        void *ptr = std::malloc(size);
        if (ptr == nullptr) {
            std::cerr << "Cannot allocate " << size << " bytes direct memory" << std::endl;
            throw std::bad_alloc();
        }
        return ptr;
    }

    std::string getNameFromColumnType(ColumnType columnType) {
        switch (columnType) {
            case COLUMN_TYPE_STRING:
                return "COLUMN_TYPE_STRING";
            case COLUMN_TYPE_INTEGER:
                return "COLUMN_TYPE_INTEGER";
            case COLUMN_TYPE_DOUBLE_FLOAT:
                return "COLUMN_TYPE_DOUBLE_FLOAT";
            default:
                return "COLUMN_TYPE_UNINITIALIZED";
        }
    }

    ColumnType getColumnTypeFromString(const std::string &str) {
        if ("COLUMN_TYPE_STRING" == str) {
            return COLUMN_TYPE_STRING;
        }
        if ("COLUMN_TYPE_INTEGER" == str) {
            return COLUMN_TYPE_INTEGER;
        }
        if ("COLUMN_TYPE_DOUBLE_FLOAT" == str) {
            return COLUMN_TYPE_DOUBLE_FLOAT;
        }
        return COLUMN_TYPE_UNINITIALIZED;
    }

    ColumnValue::ColumnValue() : columnType(COLUMN_TYPE_UNINITIALIZED), columnData(nullptr) {
    }

    ColumnValue::ColumnValue(int32_t value)
            : columnType(COLUMN_TYPE_INTEGER) {
        columnData = (char *) malloc(sizeof(int32_t));
        *((int32_t *) columnData) = value;
    }

    ColumnValue::ColumnValue(double_t value)
            : columnType(COLUMN_TYPE_DOUBLE_FLOAT) {
        columnData = (char *) malloc(sizeof(double_t));
        *((double_t *) columnData) = value;
    }

    ColumnValue::ColumnValue(const char *valuePtr, int32_t valueLength)
            : columnType(COLUMN_TYPE_STRING) {
        columnData = (char *) malloc(sizeof(int32_t) + (size_t) valueLength);
        *((int32_t *) columnData) = valueLength;
        std::memcpy(columnData + sizeof(int32_t), valuePtr, (size_t) valueLength);
    }

    ColumnValue::ColumnValue(const std::string &value)
            : columnType(COLUMN_TYPE_STRING) {
        columnData = (char *) malloc(sizeof(int32_t) + value.size());
        *((int32_t *) columnData) = (int32_t) value.size();
        std::memcpy(columnData + sizeof(int32_t), value.data(), value.size());
    }

    ColumnValue::ColumnValue(const std::vector<char> &value)
            : columnType(COLUMN_TYPE_STRING) {
        columnData = (char *) malloc(sizeof(int32_t) + value.size());
        *((int32_t *) columnData) = (int32_t) value.size();
        std::memcpy(columnData + sizeof(int32_t), value.data(), value.size());
    }

    ColumnValue::ColumnValue(const ColumnValue &rhs)
            : columnType(rhs.columnType) {
        int32_t dataSize = rhs.getRawDataSize();
        columnData = (char *) malloc((size_t) dataSize);
        std::memcpy(columnData, rhs.columnData, dataSize);
    }

    ColumnValue::ColumnValue(ColumnValue &&rhs) noexcept : columnType(COLUMN_TYPE_UNINITIALIZED), columnData(nullptr) {
        std::swap(columnType, rhs.columnType);
        std::swap(columnData, rhs.columnData);
    }

    ColumnType ColumnValue::getColumnType() const {
        return columnType;
    }

    int ColumnValue::getIntegerValue(int32_t &value) const {
        if (columnType != COLUMN_TYPE_INTEGER) {
            return -1;
        }
        value = *((int32_t *) columnData);
        return 0;
    }

    int ColumnValue::getDoubleFloatValue(double_t &value) const {
        if (columnType != COLUMN_TYPE_DOUBLE_FLOAT) {
            return -1;
        }
        value = *((double_t *) columnData);
        return 0;
    }

    int ColumnValue::getStringValue(std::pair<int32_t, const char *> &lengthStrPair) const {
        if (columnType != COLUMN_TYPE_STRING) {
            return -1;
        }
        if (lengthStrPair.second != nullptr) {
            return -2;
        }
        lengthStrPair.first = *((int32_t *) columnData);
        lengthStrPair.second = columnData + sizeof(int32_t);
        return 0;
    }

    int32_t ColumnValue::getRawDataSize() const {
        if (columnType == COLUMN_TYPE_INTEGER) {
            return (int32_t) sizeof(int32_t);
        }
        if (columnType == COLUMN_TYPE_DOUBLE_FLOAT) {
            return (int32_t) sizeof(double_t);
        }
        if (columnType == COLUMN_TYPE_STRING) {
            int32_t strLength = *((int32_t *) columnData);
            return (int32_t) sizeof(int32_t) + strLength;
        }
        return 0;
    }

    bool ColumnValue::operator==(const ColumnValue &rhs) const {
        if (this == &rhs) {
            return true;
        }
        if (columnType != rhs.columnType) {
            return false;
        }
        if (columnType == COLUMN_TYPE_INTEGER) {
            return std::memcmp(columnData, rhs.columnData, sizeof(int32_t)) == 0;
        }
        if (columnType == COLUMN_TYPE_DOUBLE_FLOAT) {
            return std::memcmp(columnData, rhs.columnData, sizeof(double_t)) == 0;
        }
        if (columnType == COLUMN_TYPE_STRING) {
            int32_t lhsRawSize = getRawDataSize();
            int32_t rhsRawSize = rhs.getRawDataSize();
            return lhsRawSize == rhsRawSize
                   && std::memcmp(columnData + sizeof(int32_t),
                                   rhs.columnData + sizeof(int32_t),
                                   lhsRawSize - sizeof(int32_t)) == 0;
        }
        return false;
    }

    bool ColumnValue::operator!=(const ColumnValue &rhs) const {
        return !(*this == rhs);
    }

    ColumnValue& ColumnValue::operator=(const ColumnValue &rhs) {
        if (this == &rhs) {
            return *this;
        }
        release(*this);
        columnType = rhs.columnType;
        int32_t dataSize = rhs.getRawDataSize();
        columnData = (char *) malloc((size_t) dataSize);
        std::memcpy(columnData, rhs.columnData, dataSize);
        return *this;
    }

    ColumnValue::~ColumnValue() {
        release(*this);
    }

}
