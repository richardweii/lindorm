#ifndef _ENV_H_
#define _ENV_H_
#include "block.h"
#include "io/writer.h"
#include "util/rte_ring.h"
#include <atomic>
#include <memory>
#include <vector>

namespace LindormContest {

class Env {
  constexpr static int kRingNum = 64;

public:
  static Env &instance() {
    static Env env;
    return env;
  }

  // file operation
  void set_root_path(std::string path);

  void create_files_for_write(int num = 512);
  Writer *get_writer(uint64_t hash);
  Writer *get_meta_writer();

  int max_file() const { return max_file_; };

  // block operation
  Status alloc_blocks(int num);
  Status release_block(Block *block);
  Status fetch_block(OUT Block **block);

private:
  Env();
  ~Env();

private:
  std::string root_path_{};
  const char *data_path_ = "data";
  const char *meta_path_ = "index";
  std::vector<File *> files_;
  bool db_file_inited_;
  bool index_file_inited_;
  int max_file_{0};

  std::atomic_int32_t fetch_pos_{};
  std::atomic_int32_t relase_pos_{};
  Block *blocks_;
  RteRing<Block, RteRingMode::MPMC> rings_[kRingNum];
  using RteRingT = RteRing<Block, 3, 4096>;
};
} // namespace LindormContest
#endif