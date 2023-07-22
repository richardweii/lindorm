//
// Header file of a sample implement of TSDBEngine.
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

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINESAMPLE_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINESAMPLE_H

#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "TSDBEngine.hpp"
#include "Hasher.hpp"

namespace LindormContest {

    /**
     * A very simple implement of TSDBEngine with very bad performance and all protocol fulfilled.
     * All data is stored in memory, and when shutdown() is called, persist data into disk.
     */
    class TSDBEngineSample : public TSDBEngine {
    public:
        explicit TSDBEngineSample(const std::string& dataDirPath);

        int connect() override;

        int createTable(const std::string &tableName, const Schema &schema) override;

        int shutdown() override;

        int upsert(const WriteRequest &wReq) override;

        int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

        int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

        ~TSDBEngineSample() override;

    public:
        std::unordered_map<std::string, Schema> schemas;
        std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>> data;
        std::mutex mutex;
        bool connected;
    }; // End class TSDBEngineSample.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINESAMPLE_H
