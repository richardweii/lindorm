#include <fcntl.h>
#include <malloc.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "compress.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/libaio.h"
#include "util/logging.h"
#include "util/util.h"

#define MAX_EVENT 10
#define BUF_LEN 512

void callback(io_context_t ctx, struct iocb* iocb, long res, long res2) { printf("test call\n"); }

int play_libaio() {
  int fd = open("/home/wei/test.txt", O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR);
  io_context_t io_context;
  struct iocb io, *p = &io;
  struct io_event event[MAX_EVENT];
  void* buf;
  posix_memalign(&buf, BUF_LEN, BUF_LEN);
  memset(buf, 0, BUF_LEN);
  // strcpy((char*)buf, "hello libaio");
  memset(&io_context, 0, sizeof(io_context));

  if (io_setup(128, &io_context)) {
    printf("io_setup error");
    return 0;
  }
  if (fd < 0) {
    printf("open file error");
    return 0;
  }
  io_prep_pread(&io, fd, buf, BUF_LEN, 0);
  io_set_callback(&io, callback);
  if (io_submit(io_context, 1, &p) < 0) {
    printf("io_submit error");
    return 0;
  }

  int num = io_getevents(io_context, 1, MAX_EVENT, event, NULL);
  for (int i = 0; i < num; i++) {
    io_callback_t io_callback = (io_callback_t)event[i].data;
    // LOG_INFO("write %d", event[i].res);
    LOG_ASSERT(event[i].res > 0, "res %zu", event[i].res);
    io_callback(io_context, event[i].obj, event[i].res, event[i].res2);
  }

  printf("%s", (char*)buf);

  close(fd);
  return 0;
}

#include <bitset>
#include <iostream>
#include <vector>

const size_t ARRAY_SIZE = 5; // Size of the arrays

void play_quick_sort() {
  uint16_t vid[ARRAY_SIZE] = {2, 1, 3, 1, 2};
  int64_t ts[ARRAY_SIZE] = {5, 4, 3, 6, 2};
  uint16_t idx[ARRAY_SIZE] = {3, 1, 4, 2, 5};

  LindormContest::quickSort(vid, ts, idx, 0, ARRAY_SIZE - 1);

  std::cout << "Sorted vid:" << std::endl;
  for (int v : vid) {
    std::cout << v << " ";
  }
  std::cout << std::endl;

  std::cout << "Sorted ts:" << std::endl;
  for (int t : ts) {
    std::cout << t << " ";
  }
  std::cout << std::endl;

  std::cout << "Sorted idx:" << std::endl;
  for (int i : idx) {
    std::cout << i << " ";
  }
  std::cout << std::endl;
}

void play_binary_serch() {
  uint16_t vid[] = {1, 1, 1, 2, 2, 2, 2, 2, 3};
  int64_t ts[] = {2, 4, 6, 1, 2, 3, 4, 5, 3};
  uint16_t idx[] = {0, 1, 2, 4, 3, 5, 6, 7, 8};
  int size = sizeof(vid) / sizeof(vid[0]);

  int target_vid = 2;
  int ts_lower = 2;
  int ts_upper = 5;

  std::vector<uint16_t> found_indices;
  std::vector<int64_t> tss;
  LindormContest::findMatchingIndices(vid, ts, idx, size, target_vid, ts_lower, ts_upper, found_indices, tss);

  if (!found_indices.empty()) {
    std::cout << "Found at indices: ";
    for (size_t index : found_indices) {
      std::cout << index << " ";
    }
    std::cout << std::endl;
  } else {
    std::cout << "Not found." << std::endl;
  }
}

class ArithmeticEncoder {
public:
  std::string encode(const std::vector<int>& data) {
    // Calculate symbol frequencies
    std::map<int, int> frequencies;
    for (int val : data) {
      if (val >= 0 && val <= 255) {
        frequencies[val]++;
      } else {
        std::cerr << "Input value " << val << " is out of range [0, 255]." << std::endl;
      }
    }

    // Build cumulative frequency table
    int totalSymbols = data.size();
    int cumulativeFrequency = 0;
    for (auto& entry : frequencies) {
      int symbol = entry.first;
      int frequency = entry.second;
      cumulativeFrequency += frequency;
      cumulativeFrequencies[symbol] = cumulativeFrequency / static_cast<double>(totalSymbols);
    }

    // Encode the data
    double low = 0.0;
    double high = 1.0;
    for (int val : data) {
      double range = high - low;
      high = low + range * cumulativeFrequencies[val];
      low = low + range * cumulativeFrequencies[val - 1];
    }

    // Convert the encoded range to binary
    std::string encodedData = doubleToBinary(low);

    return encodedData;
  }

private:
  std::map<int, double> cumulativeFrequencies;

  std::string doubleToBinary(double value) {
    std::string binary;
    for (int i = 0; i < 32; ++i) {
      value *= 2;
      int bit = static_cast<int>(value);
      binary += std::to_string(bit);
      if (value >= 1) {
        value -= 1;
      }
    }
    return binary;
  }
};

#define TO_DOUBLE(a) *((double*)(&a))

int cnt = 0;
struct Foo {
  Foo() {
    a = cnt++;
    ::printf("ctor :%p, foo %d\n", this, a);
  }

  Foo(const Foo& f) { ::printf("copy ctor this:%p, f:%p\n", this, &f); }

  Foo(const Foo&& f) { ::printf("move ctor this:%p, f:%p\n", this, &f); }

  ~Foo() { ::printf("dtor :%p, foo %d\n", this, a); }

  int a{0};
  bool operator==(const Foo& f) const { return f.a == a; }
};

struct FooHasher {
  size_t operator()(const Foo& f) const { return f.a; }
};

struct FooEqualer {
  bool operator()(const Foo& f1, const Foo& f2) const { return f1.a == f2.a; }
};

std::unordered_map<Foo, std::list<Foo>::iterator, FooHasher, FooEqualer> cache;
std::list<Foo> ll;
std::list<Foo> ll2;

void moveNode(const Foo& node, std::list<Foo>& from, std::list<Foo>& to) {
  from.erase(cache[node]);
  to.push_front(node);
  cache[node] = to.begin();
}

int main() {
  Foo f;
  ll.push_front(f);

  cache[f] = ll.begin();
  auto iter = cache.find(f);
  auto node = *iter->second;
  moveNode(node, ll, ll2);
  return 0;
}