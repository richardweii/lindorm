#pragma once

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <set>
#include <vector>

namespace LindormContest {

struct BlockMeta;
struct Interval {
  int64_t start;
  int64_t end;
  BlockMeta* meta;

  // Define comparison operator
  bool operator<(const Interval &other) const {
    return start < other.start || (start == other.start && end < other.end);
  }
};

struct Node {
  Interval range;
  int64_t max_end;
  int height;
  Node *left;
  Node *right;
  Node(const Interval &interval) : range(interval), max_end(interval.end), height(1), left(nullptr), right(nullptr) {}
};

class IntervalTree {
private:
  Node *root;

  int getHeight(Node *node) { return (node) ? node->height : 0; }

  int getBalance(Node *node) { return (node) ? getHeight(node->left) - getHeight(node->right) : 0; }

  void updateMaxEnd(Node *node) {
    if (node) {
      node->max_end = std::max(node->range.end, std::max(getMaxEnd(node->left), getMaxEnd(node->right)));
    }
  }

  Node *rotateRight(Node *y) {
    Node *x = y->left;
    Node *T2 = x->right;

    x->right = y;
    y->left = T2;

    y->height = std::max(getHeight(y->left), getHeight(y->right)) + 1;
    x->height = std::max(getHeight(x->left), getHeight(x->right)) + 1;

    updateMaxEnd(y);
    updateMaxEnd(x);

    return x;
  }

  Node *rotateLeft(Node *x) {
    Node *y = x->right;
    Node *T2 = y->left;

    y->left = x;
    x->right = T2;

    x->height = std::max(getHeight(x->left), getHeight(x->right)) + 1;
    y->height = std::max(getHeight(y->left), getHeight(y->right)) + 1;

    updateMaxEnd(x);
    updateMaxEnd(y);

    return y;
  }

  Node *balance(Node *node, const Interval &interval) {
    int balanceFactor = getBalance(node);

    if (balanceFactor > 1 && interval.start < node->left->range.start) {
      return rotateRight(node);
    }

    if (balanceFactor < -1 && interval.start > node->right->range.start) {
      return rotateLeft(node);
    }

    if (balanceFactor > 1 && interval.start > node->left->range.start) {
      node->left = rotateLeft(node->left);
      return rotateRight(node);
    }

    if (balanceFactor < -1 && interval.start < node->right->range.start) {
      node->right = rotateRight(node->right);
      return rotateLeft(node);
    }

    return node;
  }

  Node *insertNode(Node *node, const Interval &interval) {
    if (!node) {
      return new Node(interval);
    }

    if (interval.start < node->range.start) {
      node->left = insertNode(node->left, interval);
    } else {
      node->right = insertNode(node->right, interval);
    }

    node->height = 1 + std::max(getHeight(node->left), getHeight(node->right));
    node->max_end = std::max(node->range.end, std::max(getMaxEnd(node->left), getMaxEnd(node->right)));

    return balance(node, interval);
  }

  int64_t getMaxEnd(Node *node) { return (node) ? node->max_end : 0; }

  void searchOverlap(Node *node, const Interval &query, std::set<Interval> &result) {
    if (!node) {
      return;
    }

    if (!(node->range.start >= query.end || node->range.end < query.start)) {
      result.insert(node->range);
    }

    if (node->left && query.start <= getMaxEnd(node->left)) {
      searchOverlap(node->left, query, result);
    }

    if (node->right && query.end >= node->right->range.start) {
      searchOverlap(node->right, query, result);
    }
  }

public:
  IntervalTree() : root(nullptr) {}

  void insert(const Interval &interval) { root = insertNode(root, interval); }

  std::set<Interval> searchOverlap(const Interval &query) {
    std::set<Interval> result;
    searchOverlap(root, query, result);
    return result;
  }

};

} // namespace LindormContest
