#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/logging.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

int main() {
  std::string filename = "/tmp/tsdb_test/only_one_.vin2vid";
  LindormContest::SequentialReadFile file(filename);
  int num;
  file.read((char*)&num, sizeof(num));
  LOG_INFO("num %d", num);
}