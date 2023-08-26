#include "util/interval_tree.h"

using namespace LindormContest;

int main() {
  IntervalTree intervalTree;
  for (int i = 0; i < 100; i += 6) {
    intervalTree.insert({i, i + 5});
  }
  // intervalTree.insert({1, 5});
  // intervalTree.insert({2, 4});
  // intervalTree.insert({3, 9});
  // intervalTree.insert({4, 8});
  // intervalTree.insert({5, 20});
  // intervalTree.insert({6, 12});
  // intervalTree.insert({7, 15});
  // intervalTree.insert({8, 12});
  // intervalTree.insert({9, 10});
  // intervalTree.insert({10, 15});
  // intervalTree.insert({11, 15});

  Interval query = {10, 11};
  std::set<Interval> overlappingIntervals = intervalTree.searchOverlap(query);


  std::cout << "Overlapping intervals:" << std::endl;
  for (const Interval &interval : overlappingIntervals) {
    std::cout << "[" << interval.start << ", " << interval.end << "]" << std::endl;
  }

  return 0;
}
