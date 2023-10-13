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

#include "struct/CompareExpression.h"

namespace LindormContest {

    bool CompareExpression::doCompare(const ColumnValue &value1) const {
        switch (compareOp) {
            case EQUAL:
                return value == value1;
            case GREATER:
                ColumnType columnType = value1.getColumnType();
                if (columnType != value.getColumnType()) {
                    return false;
                }
                switch (columnType) {
                    int ret1, ret2;
                    case COLUMN_TYPE_INTEGER:
                        int32_t i1, i2;
                        ret1 = value1.getIntegerValue(i1);
                        ret2 = value.getIntegerValue(i2);
                        if (ret1 != 0 || ret2 != 0) {
                            std::cerr << "Cannot get COLUMN_TYPE_INTEGER type value, doCompare failed" << std::endl;
                            throw std::exception();
                        }
                        return i1 > i2;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double_t d1, d2;
                        ret1 = value1.getDoubleFloatValue(d1);
                        ret2 = value.getDoubleFloatValue(d2);
                        if (ret1 != 0 || ret2 != 0) {
                            std::cerr << "Cannot get COLUMN_TYPE_DOUBLE_FLOAT type value, doCompare failed" << std::endl;
                            throw std::exception();
                        }
                        return d1 > d2;
                    default:
                        std::cerr << "Unsupported column type for comparing: " << getNameFromColumnType(columnType) << std::endl;
                        throw std::exception();
                }
        }
        return false;
    }

}