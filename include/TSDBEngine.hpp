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

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINE_HPP
#define LINDORMTSDBCONTESTCPP_TSDBENGINE_HPP

#include <utility>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Root.h"
#include "struct/Requests.h"
#include "struct/Row.h"
#include "struct/Schema.h"

namespace LindormContest {

    class TSDBEngine {
    public:
        /**
         * Create a new instance of TSDB Engine using the designated
         * local path for it storing data. If there is no data stored
         * in this local path, a new Database would be created without
         * any data, or else, a Database would be created and all
         * existing data would be loaded.
         * @param dataDirPath The path targeting a local position, all data
         *                    of this created TS Database Engine should be
         *                    stored in this path. After we restart the
         *                    program and re-construct a new instance of
         *                    this class with the same local path, all
         *                    previous written data at this path should be
         *                    loaded and readable for the new TSDB instance.
         */
        explicit TSDBEngine(std::string dataDirPath)
                : dataDirPath(std::move(dataDirPath)) {
            struct stat64 pathStat;
            int ret = stat64(this->dataDirPath.c_str(), &pathStat);
            if (ret != 0) {
                std::cerr << "Cannot get the status of path: " << this->dataDirPath << std::endl;
                std::terminate();
            }
            ret = S_ISDIR(pathStat.st_mode);
            if (ret == 0) {
                std::cerr << "The data path is not a directory: " << this->dataDirPath << std::endl;
                std::terminate();
            }
        }

        /**
         * Start the Engine and load any data stored in data path if existing.
         * @return 0 if connect successfully.
         */
        virtual int connect() = 0;

        /**
         * Create a new table.
         * @param schema   Describe the table schema of this TSDB instance,
         *                 i.e., how many columns are there for each row,
         *                 the name of each column and what the columns'
         *                 data types are.
         * @return 0 if success.
         */
        virtual int createTable(const std::string &tableName, const Schema &schema) = 0;

        /**
         * Shutdown the Engine, after this function is returned, all dirty data
         * should be persisted, i.e., written to the data path and can be read
         * by any same-class instances using the same data path.
         */
        virtual int shutdown() = 0;

        /**
         * Write several rows to the Engine and should be readable immediately by
         * #read function of this instance. Whether or not should data be persisted
         * immediately to data path is not defined.<br>
         * Attention:<br>
         * 1. Any implements of this function should be multi-thread friendly.<br>
         * 2. If a row for the same vin and same timestamp exists, overwrite it.<br>
         * 3. After the function returns, the input parameter wReq object
         *    may be reused by the caller, the implement should not cache the
         *    wReq without any copy actions.
         * @return 0 if written successfully.
         */
        virtual int upsert(const WriteRequest &wReq) = 0;

        /**
         * Read the rows related to several vins with the newest timestamp.<br>
         * Attention:<br>
         * 1. Any implements of this function should be multi-thread friendly.<br>
         * 2. Do not return any column that was not requested.<br>
         * 3. If no column is requested (pReadReq.requestedColumns.empty() == true), return all columns.<br>
         * 4. If no data for a vin, skip this vin.
         * @return 0 if read successfully, or else whether the pReadRes parameter
         *         is modified or not is undefined. If no vin is stored in DB, and
         *         no error occurred, remain the pReadRes unmodified and return 0.
         */
        virtual int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) = 0;

        /**
         * Read rows related to the vin from trReadReq and with a timestamp [timeLowerBound, timeUpperBound).
         * timeLowerBound is included, and timeUpperBound is excluded.
         * Put the results to trReadRes and return the appended rows' num.<br>
         * Attention:<br>
         * 1. Any implements of this function should be multi-thread friendly.<br>
         * 2. Do not return any column that was not requested.<br>
         * 3. If no column is requested (trReadReq.requestedColumnFieldNames.empty() == true), return all columns.
         * @return 0 if read successfully.<br>
         *         if this vin doesn't exist or no values in the timestamp range should also return 0.<br>
         *         others means error, under which the trReadRes parameter is modified or not is undefined.
         */
        virtual int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) = 0;

        virtual ~TSDBEngine() = default;

        /**
         * Get the data path in which the TSDB Engine stores data.
         * @return The data path of this TSDB Engine.
         */
        const std::string& getDataPath() {
            return dataDirPath;
        }

    protected:
        /**
         * The data path that all data stored. No other directory should be
         * written when the Database is running, or you will be called for a foul.<br>
         * Attention:<br>
         *   1. the dataPath targets a directory.<br>
         *   2. the targeted directory has been created before the constructor is called.
         */
        const std::string dataDirPath;

    }; // End class TSDBEngine.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINE_HPP
