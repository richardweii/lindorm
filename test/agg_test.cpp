#include "agg.h"

using namespace LindormContest;

int main() {
  {
    std::vector<int> tmp{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    AvgAggregate<int> avg;
    for (auto& item : tmp) {
      avg.Add(item);
    }

    MaxAggreate<int> max;
    for (auto& item : tmp) {
      max.Add(item);
    }

    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    std::vector<int> tmp{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    AvgAggregate<int> avg(GREATER, 3);
    for (auto& item : tmp) {
      avg.Add(item);
    }

    MaxAggreate<int> max(GREATER, 11);
    for (auto& item : tmp) {
      max.Add(item);
    }

    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    AvgAggregate<int> avg;
    MaxAggreate<int> max;
    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    std::vector<double> tmp{0, 1.2, 2.3, 3.4, 4.4, 5.5, 6.5, 7.8, 8.1, 9.1, 10.2};
    AvgAggregate<double> avg;
    MaxAggreate<double> max;
    for (auto& item : tmp) {
      avg.Add(item);
    }

    for (auto& item : tmp) {
      max.Add(item);
    }
    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    AvgAggregate<double> avg;
    MaxAggreate<double> max;
    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    std::vector<double> tmp{0, 1.2, 2.3, 3.4, 4.4, 5.5, 6.5, 7.8, 8.1, 9.1, 10.2};
    AvgAggregate<double> avg(EQUAL, 1.2);
    MaxAggreate<double> max(EQUAL, 1.2);
    for (auto& item : tmp) {
      avg.Add(item);
    }

    for (auto& item : tmp) {
      max.Add(item);
    }
    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  {
    std::vector<double> tmp{0, 1.2, 2.3, 3.4, 4.4, 5.5, 6.5, 7.8, 8.1, 9.1, 10.2};
    AvgAggregate<double> avg(GREATER, 8.1);
    MaxAggreate<double> max(GREATER, 12);
    for (auto& item : tmp) {
      avg.Add(item);
    }

    for (auto& item : tmp) {
      max.Add(item);
    }
    {
      auto res = avg.GetResult();
      std::cout << "AVG : " << res << std::endl;
    }
    {
      auto res = max.GetResult();
      std::cout << "MAX : " << res << std::endl;
    }
  }
  return 0;
}