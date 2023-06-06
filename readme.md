# 基于Flink和Drools的动态规则实时营销系统

本项目是一个事件驱动型的智能实时营销推送系统。基于事件驱动，实时输出营销运营决策结果。



# 系统简介

应用场景：实时营销、实时风控

本质：当用户行为符合某种规则（动态规则）时，系统进行某种反馈。

举例：业务方可以通过“自定义营销规则”来提升销售额。比如发现满足特定条件的用户在做出某个指定行为时，为用户推送站内消息。比如，平台上架了一款新品雀巢咖啡，为了促进这款商品的销售，业务方制定一个规则：月咖啡品类的消费金额满200元（某个时间范围里，某个行为，发生次数）、用户年龄20-40岁（用户画像）、最近一个月内平均每周浏览咖啡品类商品的行为>=10次（某个时间范围里，某个行为组合，发生的次数），如果再一次浏览饮品类商品（触发事件），立即给他推送雀巢咖啡的上架消息和优惠券发放。



# 系统整体架构

- 规则引擎模块（负责规则的匹配和计算）
- 规则管理模块（往引擎里注入和动态修改规则，包括规则的修改、停用、启用、下线）
- 性能监控模块（负责对整个系统运行的关键性能进行监控（clickhouse、hbase请求数、延迟、规则的匹配次数、匹配成立次数、匹配不成立次数等）



# 规则引擎整体代码架构

- 整个系统基于flink实现

- 核心计算流程的入口是flink的keyed process function

- 规则计算所涉及的数据涉及到3个存储系统：hbase、clickhouse、state

- 具体计算一个规则条件是否匹配时，可能上述3个存储系统都要查。

- 对用户行为明细数据的查询计算，做了一个分段查询设计

  - 为什么要分段？：1、减小clickhouse的查询压力；2、数据从kafka到clickhouse之间可能存在延迟（数据延迟）

  - 怎么分段？：clickhouse里的最新数据和实际发生的最新数据之间的差距是无法精确控制的，如果做一个当前事件倒退1h的时间点作为分界点，会导致分界点会随着时间的推移而不断变化，因此设计了一个方案，分界点取当前时间向上取整，-2h。

- 上述设计思想的实现，在代码上做了javaweb开发中经典的三层架构，controller、service、dao。

![代码架构](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/代码架构.png)



# 涉及的数据类型

1、用户行为明细数据（埋点日志）

主要由3个部分组成，1、用户的终端属性（公共属性），2、事件类型，3、事件属性

2、用户画像数据

![数据模拟生成及flink程序](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/数据模拟生成及flink程序.png)



# 规则引擎的各层代码实现细节



### 1、核心计算流程的入口是flink的keyed process function

![KeyedProcessFunction](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/KeyedProcessFunction.png)



### 2、三层架构，controller、service、dao

![controller和service层](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/controller和service层.png)



### 3、DAO层实现细节HbaseQuery

<img src="/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/hbaseQuery.png" alt="hbaseQuery" style="zoom:50%;" />



### 4、DAO层实现细节ClickhouseQuery

![clickhouseQuery](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/clickhouseQuery.png)



### 5、DAO层实现细节StateQuery

![stateQuery](/Users/momochan/chapter0/vm/doit_flink/proj_bk_sec/png/stateQuery.png)

