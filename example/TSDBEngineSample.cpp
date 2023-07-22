//
// Cpp file of a sample implement of TSDBEngine.
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

#include <cstdio>
#include <fstream>
#include <algorithm>
#include <cinttypes>

#ifdef _WIN32
#include <windows.h>
#else
#include <arpa/inet.h>
#endif

#include "TSDBEngineSample.h"
#include "Hasher.hpp"

namespace LindormContest {

#define ERR_LOG(str, ...) {                                  \
    fprintf(stderr, "%s:%d. [ERROR]: ", __FILE__, __LINE__); \
    fprintf(stderr, str, ##__VA_ARGS__);                     \
    fprintf(stderr, "\n");                                   \
}

#define INFO_LOG(str, ...) {                                 \
    fprintf(stdout, "%s:%d. [INFO]: ", __FILE__, __LINE__);  \
    fprintf(stdout, str, ##__VA_ARGS__);                     \
    fprintf(stdout, "\n");                                   \
}

    /*
     *
     * Structure of data dir path:
     *   schemas; // File that stores the table name and schemas.
     *   data;    // File that stores the data.
     *
     */

    static const int32_t SCHEMA_FILE_MAGIC_NUM = -2771324;
    static const int32_t DATA_FILE_MAGIC_NUM = -32423534;

    template <typename T>
    static int readString(T &s, std::istream &in) {
        uint32_t strSizeN;
        in.read((char *) &strSizeN, sizeof(uint32_t));
        if (!in.good()) {
            ERR_LOG("IOException when reading from istream")
            return -1;
        }
        #ifdef _WIN32
        uint32_t strSize = strSizeN;
        #else
        uint32_t strSize = ntohl(strSizeN);
        #endif
        if (strSize == 0) {
            s.resize(0);
            return 0;
        }
        s.reserve(strSize + 1);
        s.resize(strSize);
        in.read((char *) s.data(), strSize);
        if (!in.good()) {
            ERR_LOG("Premature EOF or other IOException")
            return -3;
        }
        ((char *)s.data())[strSize] = '\0';
        return 0;
    }

    template <typename T>
    static int writeString(const T &s, std::ostream &out) {
        uint32_t strSize = (uint32_t) s.size();
        #ifdef _WIN32
        uint32_t strSizeN = strSize;
        #else
        uint32_t strSizeN = htonl(strSize);
        #endif
        out.write((char *) &strSizeN, sizeof(uint32_t));
        out.write(s.data(), strSize);
        if (!out.good()) {
            ERR_LOG("Write failed from IOException")
            return -2;
        }
        return 0;
    }

    static int readSignedInteger(int64_t &num, std::istream &in) {
        std::vector<char> buff;
        int ret = readString(buff, in);
        if (ret != 0) {
            return -1;
        }
        if (buff.empty()) {
            ERR_LOG("Unexpected situation, decimal number cannot write as an empty string")
            return -2;
        }
        ret = std::sscanf(buff.data(), "%" PRId64, &num);
        if (ret <= 0) {
            ERR_LOG("sscanf failed")
            return -3;
        }
        return 0;
    }

    static int writeSignedInteger(int64_t num, std::ostream &out) {
        std::vector<char> buff(1000, '\0');
        int ret = std::sprintf(buff.data(), "%" PRId64, num);
        if (ret <= 0) {
            ERR_LOG("Unexpected situation, decimal number cannot write as an empty string")
            return -1;
        }
        buff.resize(ret);
        ret = writeString(buff, out);
        if (ret != 0) {
            return -1;
        }
        return 0;
    }

    static int readSchemaFromFile(const std::string &file, std::unordered_map<std::string, Schema> &schemas) {

        /**
         *
         * File structures:
         * [magic number]
         * [table nums]
         * [table 1]
         * [table 2]
         * ...
         * [table n]
         *
         *   For each table:
         *   [column nums]
         *   [table name]
         *   [column 1]
         *   [column 2]
         *   ...
         *   [column n]
         *
         *     For each column:
         *     [column name]
         *     [column data type]
         *
         */

        struct stat64 pathStat;
        int ret = stat64(file.c_str(), &pathStat);
        if (ret != 0) {
            INFO_LOG("Cannot get the status of file: [%s]", file.c_str())
            return 0;
        }
        int64_t fileSize = pathStat.st_size;
        if (fileSize <= 0) {
            INFO_LOG("Not a file or an empty file, cannot read schema from [%s]", file.c_str())
            return 0;
        }
        std::fstream fin;
        fin.open(file.c_str(), std::ios::binary | std::ios::in);
        if (!fin.is_open() || !fin.good()) {
            INFO_LOG("Cannot read the schema file [%s], check the stat of this file", file.c_str())
            return 0;
        }

        // Verify magic number.
        int64_t magicNum = -1;
        ret = readSignedInteger(magicNum, fin);
        if (ret != 0) {
            ERR_LOG("Cannot read magic number from schema file: [%s]", file.c_str())
            fin.close();
            return -4;
        }
        if (magicNum != SCHEMA_FILE_MAGIC_NUM) {
            ERR_LOG("Not a legal schema file since magic number is not match: [%s]", file.c_str())
            fin.close();
            return -5;
        }

        // Table number.
        int64_t tableNumber = -1;
        ret = readSignedInteger(tableNumber, fin);
        if (ret != 0) {
            ERR_LOG("Cannot read table number from schema file: [%s]", file.c_str())
            fin.close();
            return -6;
        }
        if (tableNumber <= 0) {
            ERR_LOG("Invalid table number, this is unexpected, file: [%s]", file.c_str())
            fin.close();
            return -7;
        }

        for (int tableI = 0; tableI < tableNumber; ++tableI) {
            // Column's number.
            int64_t columnsNumber;
            ret = readSignedInteger(columnsNumber, fin);
            if (ret != 0) {
                ERR_LOG("IOException")
                fin.close();
                return -8;
            }
            if (columnsNumber <= 0) {
                ERR_LOG("Illegal column's number: [%" PRId64 "] at file: [%s]", columnsNumber, file.c_str())
                fin.close();
                return -9;
            }

            std::vector<char> buff;

            // Table name.
            ret = readString(buff, fin);
            if (ret != 0) {
                ERR_LOG("IOException")
                fin.close();
                return -10;
            }
            char tableNameRawCharPtr[buff.size() + 1];
            tableNameRawCharPtr[buff.size()] = '\0';
            memcpy(tableNameRawCharPtr, buff.data(), buff.size());
            std::string tableName(tableNameRawCharPtr);

            // Columns.
            std::map<std::string, ColumnType> columnsMap;
            for (int columnI = 0; columnI < columnsNumber; ++columnI) {
                // Column name.
                ret = readString(buff, fin);
                if (ret != 0) {
                    ERR_LOG("IOException")
                    fin.close();
                    return -11;
                }
                char columnNameRawCharPtr[buff.size() + 1];
                columnNameRawCharPtr[buff.size()] = '\0';
                memcpy(columnNameRawCharPtr, buff.data(), buff.size());
                std::string columnName(columnNameRawCharPtr);

                // Column data type.
                ret = readString(buff, fin);
                if (ret != 0) {
                    ERR_LOG("IOException")
                    fin.close();
                    return -12;
                }
                char columnTypeRawCharPtr[buff.size() + 1];
                columnTypeRawCharPtr[buff.size()] = '\0';
                memcpy(columnTypeRawCharPtr, buff.data(), buff.size());
                std::string columnTypeStr(columnTypeRawCharPtr);
                ColumnType columnType = getColumnTypeFromString(columnTypeStr);

                columnsMap.insert(std::pair<std::string, ColumnType>(std::move(columnName), columnType));
            }

            Schema schemaForTable(std::move(columnsMap));
            schemas.insert(std::pair<std::string, Schema>(std::move(tableName), std::move(schemaForTable)));
        }

        if (!fin.eof()) {
            ERR_LOG("Unrecognized file tail for schemas file: [%s]", file.c_str())
        }
        fin.close();

        return 0;
    }

    static int writeSchemaToFile(const std::string &file, const std::unordered_map<std::string, Schema> &schemas) {
        if (schemas.empty()) {
            INFO_LOG("Cannot write an empty schema")
            return 0;
        }

        std::fstream fout;
        fout.open(file.c_str(), std::ios::binary | std::ios::out | std::ios::trunc);
        if (!fout.is_open() || !fout.good()) {
            ERR_LOG("Cannot open the target file for writing schemas: [%s]", file.c_str())
            return -2;
        }

        // Magic number.
        int ret = writeSignedInteger(SCHEMA_FILE_MAGIC_NUM, fout);
        if (ret != 0) {
            fout.close();
            return -3;
        }

        // Table nums.
        ret = writeSignedInteger((int32_t) schemas.size(), fout);
        if (ret != 0) {
            fout.close();
            return -4;
        }

        std::unordered_map<std::string, Schema>::const_iterator tableIt = schemas.cbegin();
        for (; tableIt != schemas.cend(); ++tableIt) {
            const std::string &tableName = tableIt->first;
            const std::map<std::string, ColumnType> &columnsMap = tableIt->second.columnTypeMap;
            ret = writeSignedInteger((int32_t) columnsMap.size(), fout);
            if (ret != 0) {
                fout.close();
                return -5;
            }
            ret = writeString(std::vector<char>(tableName.cbegin(), tableName.cend()), fout);
            if (ret != 0) {
                fout.close();
                return -6;
            }
            std::map<std::string, ColumnType>::const_iterator columnIt = columnsMap.cbegin();
            for (; columnIt != columnsMap.cend(); ++columnIt) {
                std::string columnTypeStr = getNameFromColumnType(columnIt->second);
                ret = writeString(std::vector<char>(columnIt->first.cbegin(), columnIt->first.cend()), fout);
                if (ret != 0) {
                    fout.close();
                    return -7;
                }
                ret = writeString(std::vector<char>(columnTypeStr.cbegin(), columnTypeStr.cend()), fout);
                if (ret != 0) {
                    fout.close();
                    return -8;
                }
            }
        }

        fout.close();
        return 0;
    }

    static int readDataFromFile(const std::string &file, std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>> &data) {

        /**
         *
         * File structures:
         * [magic number]
         * [table nums]
         * [table 1]
         * [table 2]
         * ...
         * [table n]
         *
         *   For each table:
         *   [tableName]
         *   [rowNums]
         *   [row 1]
         *   [row 2]
         *   [row 3]
         *   ...
         *   [row n]
         *
         *     For each row:
         *     [vin]
         *     [ts]
         *     [columnNums]
         *     [column 1]
         *     [column 2]
         *     ...
         *     [column n]
         *
         *       For each column:
         *       [column name]
         *       [column data type]
         *       [column data length]
         *       [column data]
         */

        struct stat64 pathStat;
        int ret = stat64(file.c_str(), &pathStat);
        if (ret != 0) {
            INFO_LOG("Cannot get the status of file: [%s]", file.c_str())
            return 0;
        }
        int64_t fileSize = pathStat.st_size;
        if (fileSize <= 0) {
            INFO_LOG("Not a file or an empty file, cannot read table data from [%s]", file.c_str())
            return 0;
        }
        std::fstream fin;
        fin.open(file.c_str(), std::ios::binary | std::ios::in);
        if (!fin.is_open() || !fin.good()) {
            INFO_LOG("Cannot read the table data file [%s], check the stat of this file", file.c_str())
            return 0;
        }

        // Verify magic number.
        int64_t magicNum = -1;
        ret = readSignedInteger(magicNum, fin);
        if (ret != 0) {
            ERR_LOG("Cannot read magic number from table data file: [%s]", file.c_str())
            fin.close();
            return -4;
        }
        if (magicNum != DATA_FILE_MAGIC_NUM) {
            ERR_LOG("Not a legal table data file since magic number is not match: [%s]", file.c_str())
            fin.close();
            return -5;
        }

        // Table nums.
        int64_t tableNums = -1;
        ret = readSignedInteger(tableNums, fin);
        if (ret != 0) {
            fin.close();
            return -6;
        }
        if (tableNums <= 0) {
            ERR_LOG("O table, should not have table data file")
            fin.close();
            return -7;
        }

        for (int tableI = 0; tableI < tableNums; ++tableI) {
            // Table name.
            std::string tableName;
            ret = readString(tableName, fin);
            if (ret != 0) {
                fin.close();
                return -7;
            }
            // Row nums.
            int64_t rowNums = -1;
            ret = readSignedInteger(rowNums, fin);
            if (ret != 0) {
                fin.close();
                return -7;
            }
            if (rowNums <= 0) {
                ERR_LOG("Invalid row num: [%" PRId64 "]", rowNums)
                fin.close();
                return -8;
            }
            std::unordered_set<Row, RowHasher, RowHasher> tableRows;
            tableRows.reserve(rowNums);
            std::vector<char> buff;
            for (int64_t rowI = 0; rowI < rowNums; ++rowI) {
                ret = readString(buff, fin);
                if (ret != 0) {
                    fin.close();
                    return -9;
                }
                if (buff.size() != VIN_LENGTH) {
                    ERR_LOG("Try reading vin but got a string length not equals 17")
                    fin.close();
                    return -10;
                }
                Row row;
                Vin &vin = row.vin;
                memcpy(vin.vin, buff.data(), VIN_LENGTH);
                int64_t &timestamp = row.timestamp;
                ret = readSignedInteger(timestamp, fin);
                if (ret != 0) {
                    fin.close();
                    return -11;
                }
                int64_t columnNums;
                ret = readSignedInteger(columnNums, fin);
                if (ret != 0) {
                    fin.close();
                    return -12;
                }
                if (columnNums <= 0) {
                    ERR_LOG("Non positive column nums")
                    fin.close();
                    return -13;
                }
                for (int columnI = 0; columnI < columnNums; ++columnI) {
                    std::string columnName;
                    ret = readString(columnName, fin);
                    if (ret != 0) {
                        fin.close();
                        return -14;
                    }
                    std::string columnTypeStr;
                    ret = readString(columnTypeStr, fin);
                    if (ret != 0) {
                        fin.close();
                        return -15;
                    }
                    ColumnType columnType = getColumnTypeFromString(columnTypeStr);
                    int64_t columnDataRawLength;
                    ret = readSignedInteger(columnDataRawLength, fin);
                    if (ret != 0) {
                        fin.close();
                        return -16;
                    }
                    if (columnDataRawLength <= 0) {
                        ERR_LOG("columnDataRawLength <= 0")
                        fin.close();
                        return -17;
                    }
                    std::vector<char> columnData;
                    ret = readString(columnData, fin);
                    if (ret != 0) {
                        fin.close();
                        return -18;
                    }
                    if ((int64_t) columnData.size() != columnDataRawLength) {
                        ERR_LOG("Actual read column data length doesn't match the length we read")
                        fin.close();
                        return -19;
                    }
                    ColumnValue columnValue;
                    columnValue.columnType = columnType;
                    columnValue.columnData = (char *) std::malloc(columnDataRawLength);
                    if (columnValue.columnData == nullptr) {
                        ERR_LOG("Cannot alloc [%" PRId64 "] direct memory", columnDataRawLength)
                        fin.close();
                        return -20;
                    }

                    /*
                     * The persistence of ColumnValue in this demo does not take into account the format of
                     * in-memory data storage between different architectures of computers, such as the differences
                     * of whether Big Endian or Little Endian is used, and the definition of floating point
                     * numbers. The actual formatting may require consideration of cross-platform compatibility.
                     */

                    memcpy(columnValue.columnData, columnData.data(), columnDataRawLength);
                    row.columns.insert(std::pair<std::string, ColumnValue>(std::move(columnName), std::move(columnValue)));
                }
                tableRows.insert(std::move(row));
            }
            data.insert(std::pair<std::string, std::unordered_set<Row, RowHasher, RowHasher>>(std::move(tableName), std::move(tableRows)));
        }

        fin.close();
        return 0;
    }

    static int writeDataToFile(const std::string &file, const std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>> &data) {
        if (data.empty()) {
            INFO_LOG("Cannot write an empty data")
            return 0;
        }

        std::fstream fout;
        fout.open(file.c_str(), std::ios::binary | std::ios::out | std::ios::trunc);
        if (!fout.is_open() || !fout.good()) {
            ERR_LOG("Cannot open the target file for writing data: [%s]", file.c_str())
            return -2;
        }

        // Magic number.
        int ret = writeSignedInteger(DATA_FILE_MAGIC_NUM, fout);
        if (ret != 0) {
            fout.close();
            return -3;
        }

        // Table nums.
        ret = writeSignedInteger((int32_t) data.size(), fout);
        if (ret != 0) {
            fout.close();
            return -4;
        }

        std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>>::const_iterator tableIt = data.cbegin();
        for (; tableIt != data.cend(); ++tableIt) {
            const std::string &tableName = tableIt->first;
            const std::unordered_set<Row, RowHasher, RowHasher> &rows = tableIt->second;
            ret = writeString(tableName, fout);
            if (ret != 0) {
                fout.close();
                return -5;
            }
            ret = writeSignedInteger((int64_t) rows.size(), fout);
            if (ret != 0) {
                fout.close();
                return -6;
            }
            std::unordered_set<Row, RowHasher, RowHasher>::const_iterator rowIt = rows.cbegin();
            for (; rowIt != rows.cend(); ++rowIt) {
                std::vector<char> vinVector(VIN_LENGTH);
                std::memcpy(vinVector.data(), rowIt->vin.vin, VIN_LENGTH);
                ret = writeString(vinVector, fout);
                if (ret != 0) {
                    fout.close();
                    return -7;
                }
                ret = writeSignedInteger(rowIt->timestamp, fout);
                if (ret != 0) {
                    fout.close();
                    return -8;
                }
                ret = writeSignedInteger((int64_t) rowIt->columns.size(), fout);
                if (ret != 0) {
                    fout.close();
                    return -9;
                }
                std::map<std::string, ColumnValue>::const_iterator columnIt = rowIt->columns.cbegin();
                for (; columnIt != rowIt->columns.cend(); ++columnIt) {
                    const std::string &columnName = columnIt->first;
                    const ColumnValue &columnValue = columnIt->second;
                    ret = writeString(columnName, fout);
                    if (ret != 0) {
                        fout.close();
                        return -10;
                    }
                    std::string columnTypeStr = getNameFromColumnType(columnValue.columnType);
                    ret = writeString(columnTypeStr, fout);
                    if (ret != 0) {
                        fout.close();
                        return -11;
                    }
                    int64_t columnRawSize = columnValue.getRawDataSize();
                    ret = writeSignedInteger(columnRawSize, fout);
                    if (ret != 0) {
                        fout.close();
                        return -12;
                    }

                    /*
                     * The persistence of ColumnValue in this demo does not take into account the format of
                     * in-memory data storage between different architectures of computers, such as the differences
                     * of whether Big Endian or Little Endian is used, and the definition of floating point
                     * numbers. The actual formatting may require consideration of cross-platform compatibility.
                     */

                    std::vector<char> columnRawDataVector(columnRawSize);
                    memcpy(columnRawDataVector.data(), columnValue.columnData, columnRawSize);
                    ret = writeString(columnRawDataVector, fout);
                    if (ret != 0) {
                        fout.close();
                        return -13;
                    }
                }
            }
        }
        fout.close();
        return 0;
    }

    TSDBEngineSample::TSDBEngineSample(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath), schemas(), data(), mutex(), connected(false) {
    }

    int TSDBEngineSample::connect() {
        if (connected) {
            ERR_LOG("TSDB Engine has been connected")
            return -1;
        }

        int ret = readSchemaFromFile(dataDirPath + "/schemas", schemas);
        if (ret == 0) {
            // Success.
            INFO_LOG("Successfully loaded schema for [%d] tables", (int32_t) schemas.size())
        } else {
            ERR_LOG("Cannot load schema, connect to TSDB Engine fail")
            return -2;
        }

        ret = readDataFromFile(dataDirPath + "/data", data);
        if (ret == 0) {
            // Success.
            INFO_LOG("Successfully loaded data for [%d] tables", (int32_t) data.size())
        } else {
            ERR_LOG("Cannot load data, connect to TSDB Engine fail")
            return -2;
        }

        connected = true;
        INFO_LOG("TSDB Engine connected. Data path: [%s]", dataDirPath.c_str())
        return 0;
    }

    int TSDBEngineSample::createTable(const std::string &tableName, const Schema &schema) {
        std::unordered_map<std::string, Schema>::const_iterator it = schemas.find(tableName);
        if (it != schemas.cend()) {
            ERR_LOG("The table [%s] existed, cannot create as a new table", tableName.c_str())
            return -1;
        }
        schemas.insert(std::pair<std::string, Schema>(tableName, schema));
        INFO_LOG("Created new table [%s]", tableName.c_str());
        return 0;
    }

    int TSDBEngineSample::upsert(const WriteRequest &wReq) {
        mutex.lock(); // This function must be multi-thread friendly.
        std::unordered_map<std::string, Schema>::const_iterator schemaIt = schemas.find(wReq.tableName);
        if (schemaIt == schemas.cend()) {
            ERR_LOG("No such table [%s], cannot upsert to", wReq.tableName.c_str());
            mutex.unlock();
            return -1;
        }
        const Schema &schema = schemaIt->second;
        std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>>::iterator dataIt = data.find(wReq.tableName);
        if (dataIt == data.cend()) {
            data.insert(std::pair<std::string, std::unordered_set<Row, RowHasher, RowHasher>>(wReq.tableName, std::unordered_set<Row, RowHasher, RowHasher>()));
            dataIt = data.find(wReq.tableName);
        }
        std::unordered_set<Row, RowHasher, RowHasher> &rowVec = dataIt->second;
        std::vector<Row>::const_iterator toWrittenRowIt = wReq.rows.cbegin();
        for (; toWrittenRowIt != wReq.rows.cend(); ++toWrittenRowIt) {
            if (toWrittenRowIt->columns.size() != schema.columnTypeMap.size()) {
                ERR_LOG("Not a complete row")
                mutex.unlock();
                return -2;
            }

            // Automatically overwrite the row with same vin and ts.
            auto prevIt = rowVec.find(*toWrittenRowIt);
            if (prevIt != rowVec.end()) {
                rowVec.erase(prevIt);
            }
            rowVec.insert(*toWrittenRowIt);
        }
        mutex.unlock();
        return 0;
    }

    int TSDBEngineSample::shutdown() {
        if (!connected) {
            ERR_LOG("Shutdown a non-connected TSDB Engine")
            return -1;
        }

        if (!schemas.empty()) {
            // Has table.
            INFO_LOG("Start to persist [%d] table's schemas", (int32_t) schemas.size())
            int ret = writeSchemaToFile(dataDirPath + "/schemas", schemas);
            if (ret != 0) {
                ERR_LOG("Cannot persist schema to file")
                return -2;
            }
            INFO_LOG("Successfully persisted [%d] table's schemas", (int32_t) schemas.size())
        }

        if (!data.empty()) {
            INFO_LOG("Start to persist [%d] table's data", (int32_t) data.size())
            int ret = writeDataToFile(dataDirPath + "/data", data);
            if (ret != 0) {
                ERR_LOG("Cannot persist data to file")
                return -2;
            }
            INFO_LOG("Successfully persisted [%d] table's data", (int32_t) data.size())
        }

        schemas.clear();
        data.clear();
        connected = false;
        INFO_LOG("Finish shutting down TSDB Engine")
        return 0;
    }

    int TSDBEngineSample::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        mutex.lock();
        const std::string &tableName = pReadReq.tableName;
        std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>>::const_iterator tableIt = data.find(tableName);
        if (tableIt == data.cend()) {
            ERR_LOG("No such table: [%s]", tableName.c_str());
            mutex.unlock();
            return -1;
        }
        std::map<Vin, Row> resMap;
        std::unordered_set<Row, RowHasher, RowHasher>::const_iterator rowIt = tableIt->second.cbegin();
        for (; rowIt != tableIt->second.cend(); ++rowIt) {
            const Row &curRow = *rowIt;
            if (std::find(pReadReq.vins.cbegin(), pReadReq.vins.cend(), curRow.vin) != pReadReq.vins.cend()) {
                auto resMapIt = resMap.find(curRow.vin);
                if (resMapIt == resMap.cend()) {
                    // no such vin in res map, add this row to map.
                    resMap.insert(std::make_pair(Vin(curRow.vin), Row(curRow)));
                } else if (resMapIt->second.timestamp < curRow.timestamp) {
                    // compare the ts, and only retain the latest ts row.
                    resMap.erase(curRow.vin);
                    resMap.insert(std::make_pair(Vin(curRow.vin), Row(curRow)));
                }
            }
        }
        for (auto resMapIt2 = resMap.begin(); resMapIt2 != resMap.end(); ++resMapIt2) {
            Row r = resMapIt2->second;
            if (!pReadReq.requestedColumns.empty()) { // If no column requested, return all columns.
                auto rcIt = r.columns.begin();
                for (; rcIt != r.columns.end(); ) {
                    if (pReadReq.requestedColumns.find(rcIt->first) == pReadReq.requestedColumns.cend()) {
                        // do not request this column, delete.
                        rcIt = r.columns.erase(rcIt);
                    } else {
                        ++rcIt;
                    }
                }
            }
            pReadRes.push_back(std::move(r));
        }
        mutex.unlock();
        return 0;
    }

    int TSDBEngineSample::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq,
                                                std::vector<Row> &trReadRes) {
        mutex.lock();
        const std::string &tableName = trReadReq.tableName;
        std::unordered_map<std::string, std::unordered_set<Row, RowHasher, RowHasher>>::const_iterator tableIt = data.find(tableName);
        if (tableIt == data.cend()) {
            ERR_LOG("No such table: [%s]", tableName.c_str());
            mutex.unlock();
            return -1;
        }
        std::unordered_set<Row, RowHasher, RowHasher>::const_iterator rowIt = tableIt->second.cbegin();
        for (; rowIt != tableIt->second.cend(); ++rowIt) {
            const Row &curRow = *rowIt;
            if (curRow.vin == trReadReq.vin
                && curRow.timestamp >= trReadReq.timeLowerBound
                && curRow.timestamp < trReadReq.timeUpperBound) {
                Row rToWritten = curRow;
                if (!trReadReq.requestedColumns.empty()) { // If no column requested, return all columns.
                    auto rcIt = rToWritten.columns.begin();
                    for (; rcIt != rToWritten.columns.end(); ) {
                        if (trReadReq.requestedColumns.find(rcIt->first) == trReadReq.requestedColumns.cend()) {
                            // do not request this column, delete.
                            rcIt = rToWritten.columns.erase(rcIt);
                        } else {
                            ++rcIt;
                        }
                    }
                }
                trReadRes.emplace_back(std::move(rToWritten));
            }
        }
        mutex.unlock();
        return 0;
    }

    TSDBEngineSample::~TSDBEngineSample() = default;

}