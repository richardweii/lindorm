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

#ifndef LINDORM_TSDB_CONTEST_CPP_REQUESTS_H
#define LINDORM_TSDB_CONTEST_CPP_REQUESTS_H

#include "Vin.h"
#include "Row.h"

namespace LindormContest {

    /**
     * Write several rows for this table.
     * All rows must be complete, i.e., containing all columns defined in schema.
     */
    typedef struct WriteRequest {
        std::string tableName;
        std::vector<Row> rows;
    }WriteRequest;

    /**
     * Request several target columns of this vin.
     * If requestedColumnFieldNames is empty, return all columns.
     * Return all rows with timestamp during [timeLowerBound, timeUpperBound).
     * timeLowerBound is included, timeUpperBound is excluded.
     */
    typedef struct TimeRangeQueryRequest {
        std::string tableName;
        Vin vin;
        int64_t timeLowerBound;
        int64_t timeUpperBound;
        std::set<std::string> requestedColumns;
    }TimeRangeQueryRequest;

    /**
     * Request several target columns of several vins.
     * If requestedFields is empty, return all columns.
     * Return the rows with the newest timestamp for these vins.
     */
    typedef struct LatestQueryRequest {
        std::string tableName;
        std::vector<Vin> vins;
        std::set<std::string> requestedColumns;
    }LatestQueryRequest;

}

#endif //LINDORM_TSDB_CONTEST_CPP_REQUESTS_H
