#pragma once

#include "common.h"
#include "struct/CompareExpression.h"
#include "util/likely.h"
namespace LindormContest {

template <typename TReslut, typename TCol>
class AggContainer {
public:
  AggContainer(CompareOp cmp, TCol filter_val) : need_filter_(true), cmp_(cmp), filter_(filter_val){};
  AggContainer() = default;

  void Add(TCol val) {
    if (filter(val)) {
      if (UNLIKELY(empty_)) {
        empty_ = false;
      }
      add(val);
    }
  };

  // return Nan if there nothing in container
  virtual TReslut GetResult() = 0;

protected:
  virtual void add(TCol val) = 0;

  bool filter(TCol val) {
    if (need_filter_) {
      if (cmp_ == GREATER && val > filter_) {
        return true;
      } else if (cmp_ == EQUAL && val == filter_) {
        return true;
      } else {
        return false;
      }
    }
    return true;
  }

  TReslut res_{};
  bool empty_{true};
  bool need_filter_{false};
  CompareOp cmp_{};
  TCol filter_{};
};

template <typename TCol>
class AvgAggregate : public AggContainer<double, TCol> {
public:
  AvgAggregate(CompareOp cmp, TCol filter_val) : AggContainer<double, TCol>(cmp, filter_val){};
  AvgAggregate() = default;

  // return true if the result is valid
  virtual double GetResult() override {
    if (this->empty_) {
      return kDoubleNan;
    }
    return this->res_ * 1.0 / cnt_;
  }

private:
  virtual void add(TCol val) override {
    cnt_++;
    this->res_ += val;
  }

  int cnt_{0};
};

template <typename T>
class MaxAggreate : public AggContainer<T, T> {
public:
  MaxAggreate(CompareOp cmp, T filter_val) : AggContainer<T, T>(cmp, filter_val){};
  MaxAggreate() = default;

  virtual T GetResult() override {
    if (this->empty_) {
      return kDoubleNan;
    }
    return this->res_;
  }

private:
  virtual void add(T val) override {
    if (val > this->res_) {
      this->res_ = val;
    }
  }
};

template class AvgAggregate<int>;
template class AvgAggregate<double>;

template <>
inline double MaxAggreate<double>::GetResult() {
  if (this->empty_) {
    return kDoubleNan;
  }
  return this->res_;
};

template <>
inline int MaxAggreate<int>::GetResult() {
  if (this->empty_) {
    return kIntNan;
  }
  return this->res_;
};

}; // namespace LindormContest