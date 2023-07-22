//
// You should modify this file.
//
// Refer TSDBEngineSample.cpp to ensure that you have understood
// the interface semantics correctly.
//

#include "TSDBEngineImpl.h"

namespace LindormContest {

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TSDBEngineImpl::TSDBEngineImpl(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath) {
    }

    int TSDBEngineImpl::connect() {
        return 0;
    }

    int TSDBEngineImpl::createTable(const std::string &tableName, const Schema &schema) {
        return 0;
    }

    int TSDBEngineImpl::shutdown() {
        return 0;
    }

    int TSDBEngineImpl::upsert(const WriteRequest &writeRequest) {
        return 0;
    }

    int TSDBEngineImpl::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        return 0;
    }

    int TSDBEngineImpl::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        return 0;
    }

    TSDBEngineImpl::~TSDBEngineImpl() = default;

}