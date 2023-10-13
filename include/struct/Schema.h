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

#ifndef LINDORM_TSDB_CONTEST_CPP_SCHEMA_H
#define LINDORM_TSDB_CONTEST_CPP_SCHEMA_H

#include "ColumnValue.h"

namespace LindormContest {

    /**
     * Delivered into TSDB Engine when TSDBEngine#creatTable() is called.
     * This object describe the table schema.
     */
    typedef struct Schema {
        // KEY: columnFieldName, VALUE: The column data type of this column field.
        std::map<std::string, ColumnType> columnTypeMap;

        explicit Schema();

        explicit Schema(const std::map<std::string, ColumnType> &columnTypeMap);

        explicit Schema(std::map<std::string, ColumnType> &&columnTypeMap);

        Schema(const Schema &rhs);

        Schema(Schema &&rhs) noexcept;

        bool operator==(const Schema &rhs) const;

        bool operator!=(const Schema &rhs) const;

        Schema& operator=(const Schema &rhs);
    }Schema;

}

#endif //LINDORM_TSDB_CONTEST_CPP_SCHEMA_H
