#include "db_index.h"

namespace LindormContest {

Status DbIndex::insert(const Row &row) {
  auto hash = hasher_(row.vin);
  auto i = hash % kShard;
  auto &map = index_[i];
  VinEntry *vin_ent = nullptr;
  // first find
  {
    std::shared_lock<std::shared_mutex> lk(lock_[i]);
    auto iter = map.find(row.vin);
    if (LIKELY(iter != map.end())) {
      vin_ent = iter->second;
    }
  }

  // second find and create
  if (vin_ent == nullptr) {
    std::unique_lock<std::shared_mutex> lk(lock_[i]);
    auto iter = map.find(row.vin);
    if (UNLIKELY(iter != map.end())) {
      vin_ent = iter->second;
    } else {
      vin_ent = new VinEntry(row.vin);
      vin_ent->create_writer(schema_);
      map[row.vin] = vin_ent;
    }
  }

  ENSURE(vin_ent != nullptr, "invalid VinEntry");
  return vin_ent->insert(row);
}

DbIndex::~DbIndex() {
  for (auto &map : index_) {
    for (auto &iter : map) {
      delete iter.second;
    }
  }
  delete schema_;
}

DbIndex::DbIndex(Schema &&schema) { schema_ = new InternalSchema(std::move(schema)); };
} // namespace LindormContest