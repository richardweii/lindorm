# 表数据结构
+ 参见官方赛题说明
  

# TSDBEngine 构造函数
+ 传入 dataPath，创建数据库对象
+ dataPath 为数据库存储数据的本地目录
+ 选手不可以使用 dataPath 以外的目录进行数据存储，否则成绩无效
+ dataPath 目录如果不存在程序会终止，选手测试时需要注意这一点，正式评测时保证目录存在且为空
+ TSDBEngineImpl 选手实现时，构造函数签名不能修改，因为评测程序会使用该构造函数来创建 TSDBEngineImpl 对象，但该构造函数的具体实现可以修改
  

# connect 接口
+ 加载数据库对象，如果 dataPath 内已有数据，则加载，否则创建新数据库
+ 评测程序可能会重启进程，使用相同 dataPath 加载数据库对象，并检查先前写入数据，因此选手必须实现可持久化能力
  

# createTable 接口
+ 创建一个表
+ 参数为表名以及 Schema 信息
+ 注：复赛中，只会创建一张表
  

# shutdown 接口
+ 关闭已连接的数据库（和 connect 接口组合调用）
+ 当本接口返回时，要求所有数据已落盘持久化
  

# write 接口
+ 写入若干行数据到某一表中
+ 传入表名，行（Row）对象，其中每个 Row 对象必须包含全部列，不允许空列（评测程序会保证这一点），注意长度为 0 的字符串不属于空列
+ 接口成功返回后，要求写入的数据立即可读
+ 本接口必须支持并发调用（Multi-thread friendly）
+ 如果数据库中已存在某行（vin + timestamp 组合已存在），则行为未定义（评测时不会出现这种情况）
  

# executeLatestQuery 接口
+ 获取若干 vin 的最新行（该 vin 的所有行中，timestamp 最大的一行）的某些列
+ requestedColumns 参数标记了需要获取的列的名称，未在该参数中标记的列不能返回
+ requestedColumns 如果为空，代表请求所有列
+ 如果某个 vin 在数据库中不存在，则跳过该 vin
+ 本接口必须支持并发调用（Multi-thread friendly）
+ 不考察返回结果 vector 中元素的顺序，只要所有结果都包含在 vector 中即可
  

# executeTimeRangeQuery 接口
+ 获取某一个 vin 若干列
+ 获取的列的 timestamp 应该位于 timeLowerBound 和 timeUpperBound 之间，不包括 timeUpperBound，包括 timeLowerBound
+ timeLowerBound < timeUpperBound
+ requestedColumns 参数标记了需要获取的列的名称，未在该参数中标记的列不能返回
+ 如果 vin 在数据库中不存在，返回空集合
+ 本接口必须支持并发调用（Multi-thread friendly）
+ 不考察返回结果 vector 中元素的顺序，只要所有结果都包含在 vector 中即可
  

# executeAggregateQuery 接口
+ 获取某一个 vin 在指定时间范围内的某个列的数据聚合值
+ 获取的列的 timestamp 应该位于 timeLowerBound 和 timeUpperBound 之间，不包括 timeUpperBound
+ timeLowerBound < timeUpperBound
+ columnName 为需要聚合的列的名称
+ aggregator 为聚合函数，目前只有 AVG 和 MAX 两种。返回值类型可参见`Aggregator`定义
+ 如果指定的时间范围在数据库中的数据集中不命中任何数据，则返回 空集合，否则结果中应仅包含 1 行
+ 本接口必须支持并发调用（Multi-thread friendly）
  

# executeDownsampleQuery 接口
+ 对某一个 vin 在指定时间范围内的某个列的数据基于时间进行分段聚合（降采样）
+ 获取的列的 timestamp 应该位于 timeLowerBound 和 timeUpperBound 之间，不包括 timeUpperBound
+ timeLowerBound < timeUpperBound
+ columnName 为需要聚合的列的名称
+ aggregator 为聚合函数，目前只有 AVG 和 MAX 两种。返回值类型可参见`Aggregator`定义
+ interval 时间窗口分段的窗口跨度。出于赛题简化的考虑, 评测会保证 interval 一定可以被 (timeUpperBound - timeLowerBound) 整除
+ 某一个时间窗口分段指定的时间范围在数据库中的数据集中不命中任何数据，则该时间分段所对应的行不应该包含在结果中
+ columnFilter 是一个比较表达式，用于过滤列的值，只有满足该表达式的列才会参与聚合计算。假如一个时间窗口内存在数据，但目标列的数据都不满足过滤条件，那么这个窗口仍然需要返回对应的行，只是该行的该列对应的值应该为 NaN（NaN 定义见`Aggregator`的说明），这与该时间范围内未命中任何数据的行为是不同的
+ `TimeRangeDownsampleRequest`中的`columnFilter`只作用于聚合计算时的列值过滤, 与 SQL 中作用于聚合算子的`FILTER`子句语义类似
+ 本接口必须支持并发调用（Multi-thread friendly）
+ 关于该接口行为详细说明，可参考 `TSDBEngine#executeDownsampleQuery()` 接口注释中的示例
  
