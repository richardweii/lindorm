#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace LindormContest {

inline int partition(uint16_t vid[], int64_t ts[], uint16_t idx[], int low, int high) {
  uint16_t pivot_vid = vid[high];
  int64_t pivot_ts = ts[high];
  int i = low;

  for (int j = low; j <= high - 1; ++j) {
    if (vid[j] < pivot_vid || (vid[j] == pivot_vid && ts[j] < pivot_ts)) {
      if (i != j) {
        std::swap(vid[i], vid[j]);
        std::swap(ts[i], ts[j]);
        std::swap(idx[i], idx[j]);
      }
      ++i;
    }
  }

  std::swap(vid[i], vid[high]);
  std::swap(ts[i], ts[high]);
  std::swap(idx[i], idx[high]);
  return i;
}

inline void quickSort(uint16_t vid[], int64_t ts[], uint16_t idx[], int low, int high) {
  if (low < high) {
    int pi = partition(vid, ts, idx, low, high);
    quickSort(vid, ts, idx, low, pi - 1);
    quickSort(vid, ts, idx, pi + 1, high);
  }
}

// Binary search function for ts array
inline int binarySearch(int64_t arr[], int low, int high, int64_t target) {
  while (low <= high) {
    int mid = low + (high - low) / 2;

    if (arr[mid] == target) {
      return mid;
    }

    if (arr[mid] < target) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return high; // Return the index of the last element less than the target
}

inline void findMatchingIndices(uint16_t vid[], int64_t ts[], uint16_t idx[], int size, int target_vid, int64_t ts_lower, int64_t ts_upper, std::vector<uint16_t> &idxs, std::vector<int64_t>& tss) {
  int low = 0;
  int high = size - 1;
  int vid_index = size;

  // Binary search for vid
  while (low <= high) {
    int mid = low + (high - low) / 2;

    if (vid[mid] == target_vid) {
      vid_index = mid;
      break;
    }

    if (vid[mid] < target_vid) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  if (vid_index < size) {
    // Find the range of ts values for the given vid
    int start_idx = vid_index;
    while (start_idx > 0 && vid[start_idx - 1] == target_vid) {
      start_idx--;
    }
    int end_idx = vid_index;
    while (end_idx < size - 1 && vid[end_idx + 1] == target_vid) {
      end_idx++;
    }

    // Binary search within the range of ts values
    int ts_start = binarySearch(ts, start_idx, end_idx, ts_lower);
    int ts_end = binarySearch(ts, start_idx, end_idx, ts_upper);

    for (int i = ts_start; i <= ts_end; ++i) {
      if (ts[i] >= ts_lower && ts[i] < ts_upper) {
        idxs.emplace_back(idx[i]);
        tss.emplace_back(ts[i]);
      }
    }
  }
}

} // namespace LindormContest
