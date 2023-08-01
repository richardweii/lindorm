#ifndef _DB_INDEX_H_
#define _DB_INDEX_H_

#include "struct/Vin.h"
#include "vin_entry.h"
#include <shared_mutex>
namespace LindormContest {

class DbIndex {
public:
  DbIndex(Schema &&schema);
  ~DbIndex();
  Status insert(const Row &row);

private:
  InternalSchema *schema_;
  std::shared_mutex lock_[kShard];
  std::unordered_map<Vin, VinEntry *, VinHasher> index_[kShard];
  VinHasher hasher_;
};

} // namespace LindormContest

#endif