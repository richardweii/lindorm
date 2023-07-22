//
// This is an evaluation program sample.
// The evaluation program will create a new Database using the targeted
// local disk path, then write several rows, then check the correctness
// of the written data, and then run the read test.
//
// The actual evaluation program is far more complex than the sample, e.g.,
// it might contain the restarting progress to clean all memory cache, it
// might test the memory cache strategies by a pre-warming procedure, and
// it might perform read and write tests crosswise, or even concurrently.
// Besides, as long as you write to the interface specification, you don't
// have to worry about incompatibility with our evaluation program.
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

#include <iostream>
#include <fstream>
#include <algorithm>
#include <chrono>

#include "TSDBEngineSample.h"
#include "Hasher.hpp"

// We may count the write/read protocol's time consumption.
static int64_t writeProtocolElapsedNanos = 0L;
static int64_t readProtocolElapsedNanos = 0L;

static void clearTempFolder(const std::string &dataPath);

/**
 * Create two tables, and verify the schema.
 * Then restart the tsdb engine and reload the tables and verify again.
 * This test relies on the schemas object of TSDBEngineSample class, since
 * the TSDBEngine abstract class doesn't have such object so that the actual
 * evaluation process may use different method to verify the schemas.
 */
static int schemaVerificationTask(const std::string &dataPath);

static int dataVerificationTask(const std::string &dataPath);

static int createTable(LindormContest::TSDBEngine *engine);

static int verifyTableSchema(LindormContest::TSDBEngineSample *engine);

/**
 * Write 2 tables 6 rows, and then overwrite one row.
 */
static int writeDataTo(LindormContest::TSDBEngine *engine);

/**
 * Verify the data written by {writeDataTo}.
 */
static int verifyTableData(LindormContest::TSDBEngine *engine);

int main(int argc, char **argv) {
    std::cout << "Hello, World!" << std::endl;

    #ifdef _WIN32
    std::string dataPath = "C:\\tsdb_test";
    #else
    std::string dataPath = "/tmp/tsdb_test";
    #endif
    clearTempFolder(dataPath);
    int ret = schemaVerificationTask(dataPath);
    if (ret != 0) {
        return ret;
    }

    clearTempFolder(dataPath);
    ret = dataVerificationTask(dataPath);
    if (ret != 0) {
        return ret;
    }

    std::cout << "Write consumed time: " << writeProtocolElapsedNanos << std::endl;
    std::cout << "Read consumed time: " << readProtocolElapsedNanos << std::endl;

    return 0;
}

static int schemaVerificationTask(const std::string &dataPath) {
    LindormContest::TSDBEngine *engine =
            new LindormContest::TSDBEngineSample(dataPath);
    int ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect failed" << std::endl;
        return -1;
    }
    ret = createTable(engine);
    if (ret != 0) {
        std::cerr << "Create table failed" << std::endl;
        return -1;
    }
    ret = verifyTableSchema((LindormContest::TSDBEngineSample *) engine);
    if (ret != 0) {
        std::cerr << "Verify table schema failed before we restart the engine" << std::endl;
        return -1;
    }
    engine->shutdown();
    delete engine;

    engine = new LindormContest::TSDBEngineSample(dataPath);
    ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect failed" << std::endl;
        return -1;
    }
    ret = verifyTableSchema((LindormContest::TSDBEngineSample *) engine);
    if (ret != 0) {
        std::cerr << "Verify table schema failed after we restart the engine" << std::endl;
        return -1;
    }
    engine->shutdown();
    delete engine;

    return 0;
}

static int dataVerificationTask(const std::string &dataPath) {
    LindormContest::TSDBEngine *engine =
            new LindormContest::TSDBEngineSample(dataPath);
    int ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect failed" << std::endl;
        return -1;
    }
    ret = createTable(engine);
    if (ret != 0) {
        std::cerr << "Create table failed" << std::endl;
        return -1;
    }

    ret = writeDataTo(engine);
    if (ret != 0) {
        std::cerr << "Create table failed" << std::endl;
        return -1;
    }

    ret = verifyTableData(engine);
    if (ret != 0) {
        std::cerr << "Verified table failed before we restart the engine" << std::endl;
        return -1;
    }

    // Restart and persist the data to disk.
    std::cout << "Restart the DB" << std::endl;
    engine->shutdown();
    delete engine;

    engine = new LindormContest::TSDBEngineSample(dataPath);
    ret = engine->connect();
    if (ret != 0) {
        std::cerr << "Connect failed after restarting" << std::endl;
        return -1;
    }
    ret = verifyTableData(engine);
    if (ret != 0) {
        std::cerr << "Verified table failed after we restart the engine" << std::endl;
        return -1;
    }

    engine->shutdown();
    delete engine;

    return 0;
}

static int writeDataTo(LindormContest::TSDBEngine *engine) {
    // Create 2 vins.
    int64_t tmpI64;
    LindormContest::Vin vin1;
    tmpI64 = 324524325;
    memcpy(vin1.vin, &tmpI64, sizeof(int64_t));
    memcpy(vin1.vin, &tmpI64, sizeof(int64_t));
    LindormContest::Vin vin2;
    tmpI64 = -2354;
    memcpy(vin2.vin, &tmpI64, sizeof(int64_t));
    memcpy(vin2.vin, &tmpI64, sizeof(int64_t));

    // Create 2 binary string.
    char str1[20];
    memset(str1, '1', 20);
    memcpy(str1, &tmpI64, sizeof(int64_t));
    memcpy(str1 + 8, &tmpI64, sizeof(int64_t));
    char str2[19];
    memset(str2, '1', 19);
    memcpy(str2, &tmpI64, sizeof(int64_t));
    memcpy(str2 + 9, &tmpI64, sizeof(int64_t));

    // Insert 3 rows to t1:  vin1 + ts1, vin2 + ts2, vin1 + ts3.
    LindormContest::WriteRequest wReq;
    wReq.tableName = "t1";
    LindormContest::Row row;

    // Row 1.
    row.vin = vin1;
    row.timestamp = 1;
    row.columns.insert(std::make_pair("t1c1", 100));
    row.columns.insert(std::make_pair("t1c2", 100.1));
    row.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str1, 20)));
    wReq.rows.push_back(std::move(row));

    // Row 2.
    row.vin = vin2;
    row.timestamp = 3;
    row.columns.insert(std::make_pair("t1c1", 101));
    row.columns.insert(std::make_pair("t1c2", 101.1));
    row.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str1, 20)));
    wReq.rows.push_back(std::move(row));

    // Row 3.
    row.vin = vin1;
    row.timestamp = 2;
    row.columns.insert(std::make_pair("t1c1", 102));
    row.columns.insert(std::make_pair("t1c2", 102.1));
    row.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str2, 19)));
    wReq.rows.push_back(std::move(row));

    // Execute upsert.
    auto start = std::chrono::high_resolution_clock::now();
    int ret = engine->upsert(wReq);
    auto finish = std::chrono::high_resolution_clock::now();
    int64_t elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    writeProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Upsert failed" << std::endl;
        return ret;
    }

    // Insert 3 rows to t2:  vin1 + ts1, vin2 + ts2, vin1 + ts3.
    wReq.tableName = "t2";
    wReq.rows.clear();

    // Row 1.
    row.vin = vin1;
    row.timestamp = 4;
    row.columns.insert(std::make_pair("t2c1", 103.1));
    row.columns.insert(std::make_pair("t2c2", LindormContest::ColumnValue(str1, 20)));
    row.columns.insert(std::make_pair("t2c3", 103));
    wReq.rows.push_back(std::move(row));

    // Row 2.
    row.vin = vin2;
    row.timestamp = 5;
    row.columns.insert(std::make_pair("t2c1", 104.1));
    row.columns.insert(std::make_pair("t2c2", LindormContest::ColumnValue(str1, 20)));
    row.columns.insert(std::make_pair("t2c3", 104));
    wReq.rows.push_back(std::move(row));

    // Row 3.
    row.vin = vin1;
    row.timestamp = 6;
    row.columns.insert(std::make_pair("t2c1", 105.1));
    row.columns.insert(std::make_pair("t2c2", LindormContest::ColumnValue(str2, 19)));
    row.columns.insert(std::make_pair("t2c3", 105));
    wReq.rows.push_back(std::move(row));

    // Execute insert.
    start = std::chrono::high_resolution_clock::now();
    ret = engine->upsert(wReq);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    writeProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Upsert failed" << std::endl;
        return ret;
    }

    // Overwrite table 1 vin1 + ts1.
    wReq.tableName = "t1";
    wReq.rows.clear();
    row.vin = vin1;
    row.timestamp = 1;
    row.columns.insert(std::make_pair("t1c1", 200));
    row.columns.insert(std::make_pair("t1c2", 200.1));
    row.columns.insert(std::make_pair("t1c3", LindormContest::ColumnValue(str2, 19)));
    wReq.rows.push_back(std::move(row));
    start = std::chrono::high_resolution_clock::now();
    ret = engine->upsert(wReq);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    writeProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Upsert failed" << std::endl;
        return ret;
    }

    return 0;
}

static int verifyTableData(LindormContest::TSDBEngine *engine) {
    // Create 2 vins.
    int64_t tmpI64;
    LindormContest::Vin vin1;
    tmpI64 = 324524325;
    memcpy(vin1.vin, &tmpI64, sizeof(int64_t));
    memcpy(vin1.vin, &tmpI64, sizeof(int64_t));
    LindormContest::Vin vin2;
    tmpI64 = -2354;
    memcpy(vin2.vin, &tmpI64, sizeof(int64_t));
    memcpy(vin2.vin, &tmpI64, sizeof(int64_t));

    // Create 2 binary string.
    char str1[20];
    memset(str1, '1', 20);
    memcpy(str1, &tmpI64, sizeof(int64_t));
    memcpy(str1 + 8, &tmpI64, sizeof(int64_t));
    char str2[19];
    memset(str2, '1', 19);
    memcpy(str2, &tmpI64, sizeof(int64_t));
    memcpy(str2 + 9, &tmpI64, sizeof(int64_t));

    // Execute latest query for part.
    LindormContest::LatestQueryRequest pReadReq;
    std::vector<LindormContest::Row> pReadRes;
    pReadReq.tableName = "t1";
    pReadReq.requestedColumns.insert("t1c1");
    pReadReq.vins.push_back(vin1);
    auto start = std::chrono::high_resolution_clock::now();
    int ret = engine->executeLatestQuery(pReadReq, pReadRes);
    auto finish = std::chrono::high_resolution_clock::now();
    int64_t elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Cannot query" << std::endl;
        return -1;
    }
    if (pReadRes.size() != 1) {
        std::cerr << "Latest res number is not correct" << std::endl;
        return -1;
    }
    if (pReadRes.begin()->vin != vin1 || pReadRes.begin()->timestamp != 2) {
        std::cerr << "Latest res content is not correct" << std::endl;
        return -1;
    }

    if (pReadRes.begin()->columns.size() != 1) {
        std::cerr << "Latest res's column number is not correct" << std::endl;
        return -1;
    }
    int32_t t1c1Val;
    ret = pReadRes.begin()->columns.begin()->second.getIntegerValue(t1c1Val);
    if (ret != 0 || t1c1Val != 102) {
        std::cerr << "Latest res content is not correct" << std::endl;
        return -1;
    }

    // Execute latest query for full.
    pReadReq.requestedColumns.insert("t1c2");
    pReadReq.requestedColumns.insert("t1c3");
    pReadReq.vins.push_back(vin1);
    pReadReq.vins.push_back(vin2);
    pReadRes.clear();
    start = std::chrono::high_resolution_clock::now();
    ret = engine->executeLatestQuery(pReadReq, pReadRes);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Cannot query" << std::endl;
        return -1;
    }
    if (pReadRes.size() != 2) {
        std::cerr << "Latest res number is not correct" << std::endl;
        return -1;
    }
    std::sort(pReadRes.begin(), pReadRes.end());
    LindormContest::Row &r0 = pReadRes[0];
    LindormContest::Row &r1 = pReadRes[1];
    int32_t intBuff;
    double_t doubleBuff;
    std::pair<int32_t, const char *> strBuff;
    if (r0.vin != vin1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.timestamp != 2 || r0.columns.size() != 3) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c1"].getIntegerValue(intBuff) != 0 || intBuff != 102) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c2"].getDoubleFloatValue(doubleBuff) != 0 || doubleBuff != 102.1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r0.columns["t1c3"].getStringValue(strBuff) != 0
        || strBuff.first != 19
        || std::strncmp(strBuff.second, str2, 19) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.vin != vin2) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.timestamp != 3 || r1.columns.size() != 3) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.columns["t1c1"].getIntegerValue(intBuff) != 0 || intBuff != 101) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (r1.columns["t1c2"].getDoubleFloatValue(doubleBuff) != 0 || doubleBuff != 101.1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    strBuff.second = nullptr;
    if (r1.columns["t1c3"].getStringValue(strBuff) != 0
        || strBuff.first != 20
        || std::strncmp(strBuff.second, str1, 20) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    pReadReq.tableName = "t2";
    pReadReq.requestedColumns.clear();
    pReadReq.requestedColumns.insert("t2c1");
    pReadReq.requestedColumns.insert("t2c2");
    pReadReq.requestedColumns.insert("t2c3");
    pReadRes.clear();
    start = std::chrono::high_resolution_clock::now();
    ret = engine->executeLatestQuery(pReadReq, pReadRes);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (pReadRes.size() != 2) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    // Execute time range query for part.
    LindormContest::TimeRangeQueryRequest trR;
    trR.vin = vin1;
    trR.tableName = "t1";
    trR.timeLowerBound = 1;
    trR.timeUpperBound = 2;
    trR.requestedColumns.insert("t1c1");
    std::vector<LindormContest::Row> trReadRes;
    start = std::chrono::high_resolution_clock::now();
    ret = engine->executeTimeRangeQuery(trR, trReadRes);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (trReadRes.size() != 1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (trReadRes[0].vin != vin1 || trReadRes[0].timestamp != 1
        || trReadRes[0].columns.size() != 1
        || trReadRes[0].columns.begin()->second.getIntegerValue(intBuff) != 0) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    if (intBuff != 200) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    // Execute time range query for full.
    trR.timeLowerBound = 1;
    trR.timeUpperBound = 6;
    trR.requestedColumns.clear();
    trReadRes.clear();
    start = std::chrono::high_resolution_clock::now();
    ret = engine->executeTimeRangeQuery(trR, trReadRes);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (trReadRes.size() != 2) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }
    trR.tableName = "t2";
    trR.vin = vin2;
    trReadRes.clear();
    start = std::chrono::high_resolution_clock::now();
    ret = engine->executeTimeRangeQuery(trR, trReadRes);
    finish = std::chrono::high_resolution_clock::now();
    elapsedNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(finish-start).count();
    readProtocolElapsedNanos += elapsedNanos;
    if (ret != 0) {
        std::cerr << "Query time range failed" << std::endl;
        return -1;
    }
    if (trReadRes.size() != 1) {
        std::cerr << "RES incorrect" << std::endl;
        return -1;
    }

    return 0;
}

static void clearTempFolder(const std::string &dataPath) {
    std::fstream foutSchema;
    foutSchema.open(dataPath + "/schemas", std::ios::out | std::ios::binary | std::ios::trunc);
    foutSchema.close();
    std::fstream foutData;
    foutData.open(dataPath + "/data", std::ios::out | std::ios::binary | std::ios::trunc);
    foutData.close();
}

static int createTable(LindormContest::TSDBEngine *engine) {
    LindormContest::Schema schema1;
    schema1.columnTypeMap["t1c1"] = LindormContest::COLUMN_TYPE_INTEGER;
    schema1.columnTypeMap["t1c2"] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    schema1.columnTypeMap["t1c3"] = LindormContest::COLUMN_TYPE_STRING;
    int ret = engine->createTable("t1", schema1);
    if (ret != 0) {
        std::cerr << "Create table 1 failed" << std::endl;
        return -1;
    }
    LindormContest::Schema schema2;
    schema2.columnTypeMap["t2c1"] = LindormContest::COLUMN_TYPE_DOUBLE_FLOAT;
    schema2.columnTypeMap["t2c2"] = LindormContest::COLUMN_TYPE_STRING;
    schema2.columnTypeMap["t2c3"] = LindormContest::COLUMN_TYPE_INTEGER;
    ret = engine->createTable("t2", schema2);
    if (ret != 0) {
        std::cerr << "Create table 2 failed" << std::endl;
        return -1;
    }
    return 0;
}

static int verifyTableSchema(LindormContest::TSDBEngineSample *engine) {
    size_t tableSize = engine->schemas.size();
    if (tableSize != 2) {
        std::cerr << "Expected table size: 2 but got: " << tableSize << std::endl;
        return -1;
    }
    LindormContest::Schema &schema1 = engine->schemas["t1"];
    LindormContest::Schema &schema2 = engine->schemas["t2"];
    size_t schema1Size = schema1.columnTypeMap.size();
    size_t schema2Size = schema2.columnTypeMap.size();
    if (schema1Size != 3 || schema2Size != 3) {
        std::cerr << "Schema 1 size: " << schema1Size;
        std::cerr << ", Schema 2 size: " << schema2Size << std::endl;
        return -1;
    }
    if (schema1.columnTypeMap["t1c1"] != LindormContest::COLUMN_TYPE_INTEGER
        || schema1.columnTypeMap["t1c2"] != LindormContest::COLUMN_TYPE_DOUBLE_FLOAT
        || schema1.columnTypeMap["t1c3"] != LindormContest::COLUMN_TYPE_STRING) {
        std::cerr << "Table 1 schema verification failed" << std::endl;
        return -1;
    }
    if (schema2.columnTypeMap["t2c1"] != LindormContest::COLUMN_TYPE_DOUBLE_FLOAT
        || schema2.columnTypeMap["t2c2"] != LindormContest::COLUMN_TYPE_STRING
        || schema2.columnTypeMap["t2c3"] != LindormContest::COLUMN_TYPE_INTEGER) {
        std::cerr << "Table 2 schema verification failed" << std::endl;
        return -1;
    }
    return 0;
}