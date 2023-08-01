#include "env.h"
#include "util/logging.h"

namespace LindormContest {
void Env::create_files_for_write(int num) {
  if (root_path_.empty()) {
    LOG_FATAL("root path is not set");
    return;
  }

  if (db_file_inited_) {
    LOG_ERROR("repeated init");
    return;
  }

  char tmp[256];
  max_file_ = num;
  for (int i = 0; i < num; i++) {
    // create db file
    ::sprintf(tmp, "%s/%s/FILE_%d", root_path_.c_str(), data_path_, i);
    files_.emplace_back(new Writer(tmp));
  }
  // create index file
  ::sprintf(tmp, "%s/%s/META", root_path_.c_str(), meta_path_);
  files_.emplace_back(new Writer(tmp));
  db_file_inited_ = true;
}

void Env::set_root_path(std::string path) { root_path_ = path; };

Writer *Env::get_writer(uint64_t hash) {
  auto idx = hash % max_file_;
  return dynamic_cast<Writer *>(files_[idx]);
};

Writer *Env::get_meta_writer() { return dynamic_cast<Writer *>(files_.back()); };

Status Env::alloc_blocks(int num) {
  if (UNLIKELY(num > 4096 * kRingNum)) {
    LOG_FATAL("block num %d > ring cap 4096 * %d", num, kRingNum);
    return Status::ExceedCapacity;
  }

  blocks_ = new Block[num];
  for (int i = 0; i < num; i++) {
    auto rc = rings_[i % kRingNum].enqueue(&blocks_[i]);
    ENSURE(rc == RteRingT::RTE_RING_OK, "init error");
  }
  return Status::OK;
};

Status Env::release_block(Block *block) {
  auto rc = rings_[relase_pos_.fetch_add(1, std::memory_order_relaxed) % kRingNum].enqueue(block);
  int retry = 0;
  while (UNLIKELY(rc != RteRingT::RTE_RING_OK)) {
    if (UNLIKELY(retry == 3)) {
      sched_yield();
      retry = 0;
    }
    rc = rings_[relase_pos_.fetch_add(1, std::memory_order_relaxed) % kRingNum].enqueue(block);
  }
  return Status::OK;
};

Status Env::fetch_block(OUT Block **block) {
  auto rc = rings_[fetch_pos_.fetch_add(1, std::memory_order_relaxed) % kRingNum].dequeue(block);
  int retry = 0;
  while (UNLIKELY(rc != RteRingT::RTE_RING_OK)) {
    if (UNLIKELY(retry == 3)) {
      sched_yield();
      retry = 0;
    }
    rc = rings_[fetch_pos_.fetch_add(1, std::memory_order_relaxed) % kRingNum].dequeue(block);
  }
  return Status::OK;
}

Env::~Env() {
  for (auto file : files_) {
    delete file;
  }
  delete[] blocks_;
};

} // namespace LindormContest