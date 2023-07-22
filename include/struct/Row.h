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

#ifndef LINDORM_TSDB_CONTEST_CPP_ROWVALUE_H
#define LINDORM_TSDB_CONTEST_CPP_ROWVALUE_H

#include "Vin.h"
#include "ColumnValue.h"

namespace LindormContest {

    /**
     * A row corresponds to a specific vin.
     * One vin may have several rows, where each row has its unique timestamp.
     * In upsert request, the fields contains all columns in our schema, which
     * form a complete row. In read request, the result may only contain several
     * columns according to our request.
     */
    typedef struct Row {
        Vin vin;
        int64_t timestamp;

        // For write request, this map must contain all columns defined in schema.
        // For read request, this is the result set only containing the columns we queried.
        std::map<std::string, ColumnValue> columns; // KEY: columnFieldName, VALVE: column data.

        Row(Row &&rhs) noexcept;

        Row(const Row &rhs);

        Row();

        bool operator==(const Row &rhs) const;

        bool operator!=(const Row &rhs) const;

        bool operator<(const Row &rhs) const;

        bool operator>(const Row &rhs) const;

        bool operator<=(const Row &rhs) const;

        bool operator>=(const Row &rhs) const;

        Row& operator=(const Row &rhs);

    }Row;

}

#endif //LINDORM_TSDB_CONTEST_CPP_ROWVALUE_H
