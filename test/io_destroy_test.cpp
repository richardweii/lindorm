#include "TSDBEngineImpl.h"
#include "filename.h"
#include "util/stat.h"

using namespace LindormContest;

std::vector<AsyncFile*> files;

void test1() {
  files.clear();
  std::cout << "start test 1" << std::endl;

  auto start = TIME_NOW;

  for (int i = 0; i < kVinNum; i++) {
    if (i % 100 == 0) {
      std::cout << i << std::endl;
    }
    files.push_back(new AsyncWriteFile(VinFileName(std::string("/tmp/tsdb_test"), kTableName, i)));
  }

  int i = 0;
  for (auto file : files) {
    i++;
    if (i % 100 == 0) {
      std::cout << i << std::endl;
    }
    delete file;
  }

  auto end = TIME_NOW;
  std::cout << "Time used : " << TIME_DURATION_US(start, end) / 1000 / 1000 - 10 << "s" << std::endl;
}

void test2() {
  files.clear();
  std::cout << "start test 2" << std::endl;
  auto start = TIME_NOW;

  uint16_t vid = 0;
  for (int i = 0; i < 500; i++) {
    files.push_back(new AsyncWriteFile(VinFileName(std::string("/tmp/tsdb_test"), kTableName, vid++)));
  }

  while (vid < kVinNum) {
    if (vid % 100 == 0) {
      std::cout << vid << std::endl;
    }
    auto lru = files.back();
    delete lru;
    files.back() = new AsyncWriteFile(VinFileName(std::string("/tmp/tsdb_test"), kTableName, vid++));
  }

  sleep(10);

  int i = 0;
  for (auto file : files) {
    i++;
    if (i % 100 == 0) {
      std::cout << i << std::endl;
    }
    delete file;
  }

  auto end = TIME_NOW;
  std::cout << "Time used : " << TIME_DURATION_US(start, end) / 1000 / 1000 - 10 << "s" << std::endl;
}

int main() {
  test1();
  sleep(30); // for release resource
  test2();
  return 0;
}
