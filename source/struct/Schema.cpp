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

#include "struct/Schema.h"

namespace LindormContest {

    Schema::Schema() = default;

    Schema::Schema(const std::map<std::string, ColumnType> &columnTypeMap) : columnTypeMap(columnTypeMap) {
    }

    Schema::Schema(std::map<std::string, ColumnType> &&columnTypeMap) : columnTypeMap(std::move(columnTypeMap)) {
    }

    Schema::Schema(const Schema &rhs) = default;

    Schema::Schema(Schema &&rhs) noexcept : columnTypeMap(std::move(rhs.columnTypeMap)) {
    }

    bool Schema::operator==(const Schema &rhs) const {
        return columnTypeMap == rhs.columnTypeMap;
    }

    bool Schema::operator!=(const Schema &rhs) const {
        return !(rhs == *this);
    }

    Schema& Schema::operator=(const Schema &rhs) {
        if (&rhs == this) {
            return *this;
        }
        columnTypeMap = rhs.columnTypeMap;
        return *this;
    }
}