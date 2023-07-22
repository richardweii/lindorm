# 工程简介
本工程是比赛时选手需要下载的工程，选手实现工程内 TSDBEngineImpl 类。
评测程序会将选手提交的修改后的本工程代码进行编译并调用选手实现的 TSDBEngineImpl 类的接口进行测试。
其他参赛说明及本文档均未说明或不清楚的问题可询比赛客服。
  

# 注意事项
1. 日志必须使用 std::out 或 std::err，打印到其他地方正式测评时可能无法透传出来。
2. 建议不要频繁打印日志，正式环境中日志的实现可能会产生同步、异步 IO 行为，影响选手成绩，正式比赛中对每次提交运行的总日志大小有限制。
3. 选手只能修改 TSDBEngineImpl 类，选手可以增加其他类，但不能修改已定义的其他文件。
4. 选手不能使用三方库（不含 C/C++ 标准库（libc/libc++ 等），以及一些 Linux 系统专属的系统内置库），评测程序会检查选手是否将三方库放入工程目录内，并且正式比赛时编译环境处于断网状态无法下载三方库。
5. 自建的头文件请放入 include 文件夹，如果有子文件夹，引入时，请携带 include 为根的相对路径，因为正式编译时头文件路径只认 include 目录：
   + 例如，头文件为 include/header_1.h，引入方式为 #include "header_1.h"
   + 例如，头文件为 include/my_folder/header_2.h，引入方式为 #include "my_folder/header_2.h"
6. 自建的 cpp 文件只能放入 source 目录下，可以存在子目录，因为正式编译时 cpp 搜寻目录只认 source 目录。
7. 选手提交时，将本工程打包到一个 zip 包中，zip 包应将整个 lindorm-tsdb-contest-cpp 目录打包，而不是将该目录下的内容打包，即最终 zip 包根目录中只有 lindorm-tsdb-contest-cpp 一个目录：
    + cd .xxxxx/lindorm-tsdb-contest-cpp
    + cd .. # 退回上级目录
    + add directory to zip package root: ./lindorm-tsdb-contest-cpp
8. 实际评测程序不会依赖本工程下的 CMakeLists.txt，因此选手可以随意修改 cmake 的属性。
9. 基础代码选手不可修改的部分（如一些结构体等）我们已经事先进行了 UT 测试，但仍不排除存在 BUG 的可能性，如果选手发现了问题影响参赛，请及时与我们联系。example 目录下的示例不影响参赛，不接受 BUG 报告。
10. 接口运行时抛出异常、返回值为异常返回值的，评测会立即结束，并视为运行失败。
11. 实际评测时，编译运行都将在 Linux (Alibaba alios-7) 中使用 g++ 12 & cmake 2.8.12 & GNU make 3.82 进行，编译时带 -std=c++17 参数。
    为了方便选手在自己的环境运行 example 项目查看测试结果，example 适配了其他系统，示例已在下列环境中测试：
    + Mac OS 13:
      + g++ 12 (brew install gcc@12 g++@12) 
      + cmake 3.26 
      + GNU make 3.81
    + Linux centos 7:
      + g++ 12 (使用源码编译 gcc 12.0.1) 
      + cmake 2.8.12
      + GNU Make 3.82
    + Windows 11:
      + g++.exe (GCC) 13.1.0
      + cmake version 3.27.0-rc3
      + GNU Make 4.4
      + 测试时将 w64devkit\bin 加入 PATH 后，直接使用 cmake -G "MinGW Makefiles" .、make 进行编译
      + 其他环境（如 Visual Studio）请自行测试
    + 特别注意：示例能在其他环境运行不意味着在相同环境开发的代码可以被实际评测环境执行，建议选手提交代码前先搭建自己的 Linux 环境使用对应版本编译器进行测试，以免提交后运行有问题，浪费了提交次数。
  

# 工程结构说明
+ include：放置头文件的目录
+ source: 放置 cpp 文件的目录
+ example: 示例代码
  

# 评测程序流程参考
1. 选手拉取本仓库，并实现 TSDBEngineImpl.cpp 中的各个接口函数。
2. 选手打包，并上传。
3. 评测程序将从 source 目录中扫描加载所有 cpp 文件。除 Linux 内置环境的 include 路径外，设置 include 根目录为头文件 -I 所在唯一位置（请选手注意对 include 子文件夹中头文件的引用路径）。
4. 评测程序可能会执行的操作：
    1. 写入测试。
    2. 正确性测试。
    3. 重启，清空缓存。
    4. 重新通过先前的数据目录重启数据库，数据库需要加载之前持久化的数据。
    5. 正确性测试。
    6. 读取性能测试。
    7. 压缩率测试。
  

# example 目录说明
1. example 中的 TSDBEngineSample 实现了完整的 TSDBEngine 接口语义。
2. TSDBEngineSample 所有数据全部存储在内存，但实际测评中，数据量远大于内存大小，数据根本无法全部缓存在内存。
3. TSDBEngineSample 使用 mutex 暴力满足了读写接口对多线程并发调用的需求。
4. TSDBEngineSample 的 data 成员存储所有数据，schemas 成员存储表格 schema 信息。
5. TSDBEngineSample 只在 shutdown 时将数据从内存全部落盘，这符合接口对持久化时间的语义。
6. TSDBEngineSample 在 connect 时将数据全部加载到内存，符合接口语义。
7. EvaluationSample 对 TSDBEngineSample 的接口进行了简单测试，包括各读写接口语义，持久化语义的测试，并记录了写入、查询接口接口调用级别的耗时。
8. 如果 example 测试时报 "Cannot get the status of path: /tmp/tsdb_test" 或者 "The data path is not a directory: /tmp/tsdb_test"，请先检查对应目录是否存在，或者修改 EvaluationSample main 函数中的数据目录。
9. 为了方便选手在各种平台测试运行，example 中的代码可能有类似 "#ifdef _WIN32" 的跨平台代码，但评测程序运行在 Linux 上，选手需要按照 Linux 的规范开发。
10. example 属于示例代码，可能存在不严谨之处，如持久化未考虑平台兼容性，部分头文件引入冗余等，仅供参考，选手对接口的理解应以赛题为准。
  
