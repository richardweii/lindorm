//
// You should modify this file.
//
// Refer TSDBEngineSample.h to ensure that you have understood
// the interface semantics correctly.
//

#ifndef LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
#define LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H

#include "TSDBEngine.hpp"

namespace LindormContest {

class TSDBEngineImpl : public TSDBEngine {
public:
    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    explicit TSDBEngineImpl(const std::string &dataDirPath);

    int connect() override;

    int createTable(const std::string &tableName, const Schema &schema) override;

    int shutdown() override;

    int upsert(const WriteRequest &wReq) override;

    int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

    int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

    ~TSDBEngineImpl() override;
}; // End class TSDBEngineImpl.

}

#endif //LINDORMTSDBCONTESTCPP_TSDBENGINEIMPL_H
