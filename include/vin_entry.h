#ifndef _VIN_ENTRY_H_
#define _VIN_ENTRY_H_

#include "Hasher.hpp"
#include "block.h"
#include "struct/Vin.h"

namespace LindormContest {
class VinEntry {
public:
  explicit VinEntry(const Vin &vin);
  void create_writer(InternalSchema *schema);
  Status insert(const Row &row);

private:
  VinHasher hasher_;
  BlockWriter *writer_;
  Vin vin_;
  std::vector<uint32_t> block_pos_;
};

} // namespace LindormContest
#endif