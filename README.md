# 工程简介
本工程是【复赛】比赛时选手需要下载的工程，选手实现工程内 TSDBEngineImpl 类。
评测程序会将选手提交的修改后的本工程代码进行编译并调用选手实现的 TSDBEngineImpl 类的接口进行测试。
其他参赛说明及本文档均未说明或不清楚的问题可询比赛客服。

### write latency
total: 219277 ms

```c++
Count: 30000000  Average: 33.8033  StdDev: 4896.48
Min: 0.0000  Median: 4.3399  Max: 2813565.5210
------------------------------------------------------
[       1,       2 )  230257   0.768%   0.768% 
[       2,       3 ) 3895791  12.986%  13.753% ###
[       3,       4 ) 7687427  25.625%  39.378% #####
[       4,       5 ) 9375096  31.250%  70.629% ######
[       5,       6 ) 5221371  17.405%  88.033% ###
[       6,       7 ) 1514242   5.047%  93.081% #
[       7,       8 )  681369   2.271%  95.352% 
[       8,       9 )  412623   1.375%  96.727% 
[       9,      10 )  257181   0.857%  97.585% 
[      10,      12 )  259821   0.866%  98.451% 
[      12,      14 )  195791   0.653%  99.103% 
[      14,      16 )  171378   0.571%  99.674% 
[      16,      18 )   40880   0.136%  99.811% 
[      18,      20 )   11605   0.039%  99.849% 
[      20,      25 )   25272   0.084%  99.934% 
[      25,      30 )    5457   0.018%  99.952% 
[      30,      35 )    2551   0.009%  99.960% 
[      35,      40 )     261   0.001%  99.961% 
[      40,      45 )     124   0.000%  99.962% 
[      45,      50 )      84   0.000%  99.962% 
[      50,      60 )     807   0.003%  99.965% 
[      60,      70 )     229   0.001%  99.965% 
[      70,      80 )     195   0.001%  99.966% 
[      80,      90 )     171   0.001%  99.967% 
[      90,     100 )     134   0.000%  99.967% 
[     100,     120 )     112   0.000%  99.967% 
[     120,     140 )      93   0.000%  99.968% 
[     140,     160 )      76   0.000%  99.968% 
[     160,     180 )      32   0.000%  99.968% 
[     180,     200 )       9   0.000%  99.968% 
[     200,     250 )      11   0.000%  99.968% 
[     250,     300 )       7   0.000%  99.968% 
[     300,     350 )       8   0.000%  99.968% 
[     350,     400 )      55   0.000%  99.968% 
[     400,     450 )      65   0.000%  99.969% 
[     450,     500 )      27   0.000%  99.969% 
[     500,     600 )      49   0.000%  99.969% 
[     600,     700 )      49   0.000%  99.969% 
[     700,     800 )      10   0.000%  99.969% 
[     800,     900 )      12   0.000%  99.969% 
[    1000,    1200 )       8   0.000%  99.969% 
[    1200,    1400 )       4   0.000%  99.969% 
[    1400,    1600 )       7   0.000%  99.969% 
[    1600,    1800 )       8   0.000%  99.969% 
[    1800,    2000 )      12   0.000%  99.969% 
[    2000,    2500 )       8   0.000%  99.969% 
[    2500,    3000 )       9   0.000%  99.969% 
[    3000,    3500 )      14   0.000%  99.969% 
[    3500,    4000 )       9   0.000%  99.969% 
[    4000,    4500 )      17   0.000%  99.969% 
[    4500,    5000 )       7   0.000%  99.969% 
[    5000,    6000 )      21   0.000%  99.970% 
[    6000,    7000 )      15   0.000%  99.970% 
[    7000,    8000 )      27   0.000%  99.970% 
[    8000,    9000 )      25   0.000%  99.970% 
[    9000,   10000 )      12   0.000%  99.970% 
[   10000,   12000 )      44   0.000%  99.970% 
[   12000,   14000 )     117   0.000%  99.970% 
[   14000,   16000 )      73   0.000%  99.971% 
[   16000,   18000 )    1489   0.005%  99.976% 
[   18000,   20000 )       3   0.000%  99.976% 
[   20000,   25000 )      14   0.000%  99.976% 
[   25000,   30000 )      13   0.000%  99.976% 
[   30000,   35000 )      99   0.000%  99.976% 
[   35000,   40000 )      35   0.000%  99.976% 
[   40000,   45000 )     268   0.001%  99.977% 
[   45000,   50000 )     798   0.003%  99.980% 
[   50000,   60000 )     482   0.002%  99.981% 
[   60000,   70000 )    5216   0.017%  99.999% 
[   70000,   80000 )      50   0.000%  99.999% 
[   80000,   90000 )      44   0.000%  99.999% 
[   90000,  100000 )      11   0.000%  99.999% 
[  100000,  120000 )      10   0.000%  99.999% 
[  120000,  140000 )       2   0.000%  99.999% 
[  140000,  160000 )       4   0.000%  99.999% 
[  180000,  200000 )       2   0.000%  99.999% 
[  200000,  250000 )       3   0.000%  99.999% 
[  350000,  400000 )       2   0.000%  99.999% 
[  400000,  450000 )       2   0.000%  99.999% 
[  450000,  500000 )       2   0.000%  99.999% 
[  500000,  600000 )       8   0.000%  99.999% 
[  600000,  700000 )       3   0.000%  99.999% 
[  700000,  800000 )      10   0.000%  99.999% 
[  800000,  900000 )      11   0.000%  99.999% 
[  900000, 1000000 )      13   0.000%  99.999% 
[ 1000000, 1200000 )      42   0.000%  99.999% 
[ 1200000, 1400000 )      40   0.000%  99.999% 
[ 1400000, 1600000 )      34   0.000% 100.000% 
[ 1600000, 1800000 )      38   0.000% 100.000% 
[ 1800000, 2000000 )      40   0.000% 100.000% 
[ 2000000, 2500000 )      39   0.000% 100.000% 
[ 2500000, 3000000 )       4   0.000% 100.000%
```
```
[INFO]  2023-11-03 09:49:50.182925 stat.cpp:51: col 0 col_name column_0, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.182942 stat.cpp:51: col 1 col_name column_1, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.182953 stat.cpp:51: col 2 col_name column_10, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.182963 stat.cpp:51: col 3 col_name column_11, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.182976 stat.cpp:51: col 4 col_name column_12, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.182989 stat.cpp:51: col 5 col_name column_13, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183003 stat.cpp:51: col 6 col_name column_14, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183015 stat.cpp:51: col 7 col_name column_15, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183027 stat.cpp:51: col 8 col_name column_16, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183040 stat.cpp:51: col 9 col_name column_17, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183052 stat.cpp:51: col 10 col_name column_18, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183064 stat.cpp:51: col 11 col_name column_19, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183077 stat.cpp:51: col 12 col_name column_2, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183089 stat.cpp:51: col 13 col_name column_20, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183102 stat.cpp:51: col 14 col_name column_21, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183115 stat.cpp:51: col 15 col_name column_22, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183127 stat.cpp:51: col 16 col_name column_23, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183140 stat.cpp:51: col 17 col_name column_24, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183151 stat.cpp:51: col 18 col_name column_25, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183163 stat.cpp:51: col 19 col_name column_26, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183175 stat.cpp:51: col 20 col_name column_27, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183188 stat.cpp:51: col 21 col_name column_28, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183200 stat.cpp:51: col 22 col_name column_29, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183212 stat.cpp:51: col 23 col_name column_3, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183224 stat.cpp:51: col 24 col_name column_30, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183237 stat.cpp:51: col 25 col_name column_31, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183249 stat.cpp:51: col 26 col_name column_32, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183261 stat.cpp:51: col 27 col_name column_33, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183273 stat.cpp:51: col 28 col_name column_34, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183285 stat.cpp:51: col 29 col_name column_35, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183297 stat.cpp:51: col 30 col_name column_36, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183309 stat.cpp:51: col 31 col_name column_37, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183322 stat.cpp:51: col 32 col_name column_38, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183334 stat.cpp:51: col 33 col_name column_39, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183346 stat.cpp:51: col 34 col_name column_4, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183358 stat.cpp:51: col 35 col_name column_40, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183369 stat.cpp:51: col 36 col_name column_41, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183381 stat.cpp:51: col 37 col_name column_42, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183394 stat.cpp:51: col 38 col_name column_43, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183406 stat.cpp:51: col 39 col_name column_44, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183418 stat.cpp:51: col 40 col_name column_45, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183431 stat.cpp:51: col 41 col_name column_46, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183444 stat.cpp:51: col 42 col_name column_47, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183456 stat.cpp:51: col 43 col_name column_48, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183468 stat.cpp:51: col 44 col_name column_49, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183480 stat.cpp:51: col 45 col_name column_5, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183492 stat.cpp:51: col 46 col_name column_50, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183505 stat.cpp:51: col 47 col_name column_51, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183517 stat.cpp:51: col 48 col_name column_52, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183530 stat.cpp:51: col 49 col_name column_53, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183542 stat.cpp:51: col 50 col_name column_54, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183554 stat.cpp:51: col 51 col_name column_55, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183566 stat.cpp:51: col 52 col_name column_56, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183578 stat.cpp:51: col 53 col_name column_57, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183591 stat.cpp:51: col 54 col_name column_58, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183603 stat.cpp:51: col 55 col_name column_59, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
[INFO]  2023-11-03 09:49:50.183616 stat.cpp:51: col 56 col_name column_6, col_type string, origin_sz 915 MB, compress_sz 31 MB compress rate is 0.033948
[INFO]  2023-11-03 09:49:50.183628 stat.cpp:51: col 57 col_name column_7, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183639 stat.cpp:51: col 58 col_name column_8, col_type double, origin_sz 228 MB, compress_sz 26 MB compress rate is 0.117294
[INFO]  2023-11-03 09:49:50.183651 stat.cpp:51: col 59 col_name column_9, col_type Integer, origin_sz 114 MB, compress_sz 24 MB compress rate is 0.213092
```
# 注意事项
1. 日志必须使用 std::out 或 std::err，打印到其他地方正式测评时可能无法透传出来。
2. 建议不要频繁打印日志，正式环境中日志的实现可能会产生同步、异步 IO 行为，影响选手成绩，正式比赛中对每次提交运行的总日志大小有限制。
3. 选手只能修改 TSDBEngineImpl 类，选手可以增加其他类，但不能修改已定义的其他文件。
4. 大赛官方不推荐通过链接使用三方库，如果需要使用三方库建议拷贝源码到工程中，如果需要链接使用请自行调试我们不提供支持。
5. 自建的头文件请放入 include 文件夹，如果有子文件夹，引入时，请携带 include 为根的相对路径，因为正式编译时头文件路径只认 include 目录：
   + 例如，头文件为 include/header_1.h，引入方式为 #include "header_1.h"
   + 例如，头文件为 include/my_folder/header_2.h，引入方式为 #include "my_folder/header_2.h"
   + 除了以 include 为根的相对路径，以本（cpp、c、cc）文件所在目录为相对路径引入头文件也是可以的
6. 自建的 cpp 文件只能放入 source 目录下，可以存在子目录，因为正式编译时 cpp 搜寻目录只认 source 目录。
7. 选手提交时，将本工程打包到一个 zip 包中，zip 包应将整个 lindorm-tsdb-contest-cpp 目录打包，而不是将该目录下的内容打包，即最终 zip 包根目录中只有 lindorm-tsdb-contest-cpp 一个目录：
    + cd .xxxxx/lindorm-tsdb-contest-cpp
    + cd .. # 退回上级目录
    + add directory to zip package root: ./lindorm-tsdb-contest-cpp
8. 实际评测程序不会依赖本工程下的 CMakeLists.txt，因此选手可以随意修改 cmake 的属性以满足本地调试需要。
9. 基础代码选手不可修改的部分（如一些结构体等）我们已经事先进行了 UT 测试，但仍不排除存在 BUG 的可能性，如果选手发现了问题影响参赛，请及时与我们联系。
10. 接口运行时抛出异常、返回值为异常返回值的，评测会立即结束，并视为运行失败。
  

# 工程结构说明
+ include：放置头文件的目录。
+ source: 放置 cpp 文件的目录。
  

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
  
# 架构调整
一个memtable中的所有列存在同一个文件当中

# 优化点
1. Row的拷贝构造太多了，利用移动语义减少拷贝 DONE
2. ColumnValue考虑自己重新实现一版 (不需要自己实现)
3. 压缩算法的调研 (ZSTD)
4. 异步写，io_uring, 协程（已经确定了线程模型）
5. 固定字节的列的下刷与string分开，避免下刷条数被string限制，需要改造元数据管理(经过测试，没用)
6. 性能测试编写需要提上日程（复赛再说）
7. time range性能太差，需要对vid + ts块进行排序，块内二分查找(DONE)
8. int和double压缩率太低，需要对其进行编码（这个是压缩率拉开差距的关键所在）
9. block meta链表长度会达到20，影响time range的速度，将其改造为更高效一点的数据结构 (改造成AVL 区间查询tree完成)

# 特征
int 45 string 6 double 9 一行456字节

35列最大值小于等于255
5列值特别小

有的列，数字分布不均衡

可以考虑：变长编码，霍夫曼编码，算数编码等来压缩int字段

复赛50000vin