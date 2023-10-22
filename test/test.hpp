#pragma once

#include <cstdio>
/*
测试的头文件
*/

#define OUTPUT(format, ...) printf(format, ##__VA_ARGS__)
#define ASSERT(condition, format, ...)                                                                                      \
  if (!(condition)) {                                                                                                       \
    OUTPUT("\033[;31mAssertion ' %s ' Failed!\n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, ##__VA_ARGS__); \
    abort();                                                                                                                \
  }

#define EXPECT(condition, format, ...)                                                                                 \
  if (!(condition)) {                                                                                                  \
    OUTPUT("\033[;33mExpect ' %s ' \n%s:%d: " format "\n\033[0m", #condition, __FILE__, __LINE__, ##__VA_ARGS__);      \
  }
