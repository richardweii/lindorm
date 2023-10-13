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
#include "CompareExpression.h"

namespace LindormContest {

    /**
     * An enum stands for the aggregator type. <br>
     * For the sake of simplicity, we only support AVG and MAX for this contest. <br>
     * The data type of the aggregation result is assumed as follows for simplicity: <br>
     * - AVG: DOUBLE <br>
     *        If all row were filtered, the result should be NaN (Bits as long: 0xfff0000000000000L).<br>
     *        You can get the NaN value by the following steps:<br><br>
     *            static int64_t DOUBLE_NAN_AS_LONG = 0xfff0000000000000L;<br>
                  static double_t DOUBLE_NAN = * (double_t*) (&DOUBLE_NAN_AS_LONG);<br><br>
     * - MAX: The same as the column type of the source column type<br>
     *        If all row were filtered, the result should be NaN (0x80000000).<br>
     */
    enum Aggregator {
        AVG,
        MAX
    };

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

    /**
     * Request for aggregation of some specified columns of this vin within a time range.
     * <br>
     * timeLowerBound is included, timeUpperBound is excluded [timeLowerBound, timeUpperBound).<br>
     * "aggregator" means the aggregator type.<br>
     * "columnName" means the column name for aggregation.
     */
    typedef struct TimeRangeAggregationRequest {
        std::string tableName;
        Vin vin;
        std::string columnName;
        int64_t timeLowerBound;
        int64_t timeUpperBound;
        Aggregator aggregator;
    }TimeRangeAggregationRequest;

    /**
     * Request for dividing the data within a specified time range for a given vehicle (represented by Vin)
     * into multiple windows based on a specified interval and calculating the aggregation result of requested
     * columns for each window.
     * <br>
     * timeLowerBound is included, timeUpperBound is excluded [timeLowerBound, timeUpperBound).<br>
     * "interval" means the downsample interval for each aggregation.<br>
     * "columnFilter" means the value compare expression for filtering rows,
     * the filter is applied to the requested column and takes effect during the aggregation phase only.
     */
    typedef struct TimeRangeDownsampleRequest : public TimeRangeAggregationRequest {
        int64_t interval;
        CompareExpression columnFilter;
    }TimeRangeDownsampleRequest;
}

#endif //LINDORM_TSDB_CONTEST_CPP_REQUESTS_H
