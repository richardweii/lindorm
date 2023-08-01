#include "vin_entry.h"
#include "Hasher.hpp"
#include "env.h"
#include "util/logging.h"

namespace LindormContest {

Status VinEntry::insert(const Row &row) {
  auto rc = writer_->write_row(row, nullptr);
  if (rc == Status::ExceedCapacity) {
    auto hash = hasher_(vin_);
    auto off = writer_->solidify(*Env::instance().get_writer(hash));
    block_pos_.push_back(off);
    rc = writer_->fetch_new_block();
    ENSURE(rc == Status::OK, "unexpected error");
    auto rc = writer_->write_row(row, nullptr);
    ENSURE(rc == Status::OK, "write error (2)");
  }
  return rc;
}

VinEntry::VinEntry(const Vin &vin) : vin_(vin){};

void VinEntry::create_writer(InternalSchema *schema) {
  writer_ = new BlockWriter(schema);
  ENSURE(writer_->init() == Status::OK, "block writer init failed");
}
} // namespace LindormContest