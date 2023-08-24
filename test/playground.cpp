#include "Hasher.hpp"
#include "TSDBEngineImpl.h"
#include "common.h"
#include "io/file.h"
#include "struct/ColumnValue.h"
#include "struct/Vin.h"
#include "test.hpp"
#include "util/libaio.h"
#include "util/logging.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include <fcntl.h>
#include <malloc.h>
#include <stdio.h>
#include <unistd.h>

#define MAX_EVENT 10
#define BUF_LEN 512

void callback(io_context_t ctx, struct iocb *iocb, long res, long res2) {
  printf("test call\n");
}

int main() {
  int fd = open("/home/wjp/test.txt", O_RDWR | O_DIRECT, 0);
  io_context_t io_context;
  struct iocb io, *p = &io;
  struct io_event event[MAX_EVENT];
  void *buf;
  posix_memalign(&buf, 512, 512);
  memset(buf, 0, BUF_LEN);
  strcpy((char*)buf, "hello libaio");
  memset(&io_context, 0, sizeof(io_context));

  if (io_setup(10, &io_context)) {
    printf("io_setup error");
    return 0;
  }
  if (fd < 0) {
    printf("open file error");
    return 0;
  }
  io_prep_pwrite(&io, fd, buf, 512, 0);
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

  close(fd);

  return 0;
}
