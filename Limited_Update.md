# 有限更新 Limited Update

## 概述

之前看了阿里分享的《ClickHouse在实时广告圈人业务中的最佳实践》文章中提到的AggregatingMergeTree结合SimpleAggregateFunction + anyLast实现Insert替换Update，来达到数据更新，受到了很大的启发。仔细研究了一下，发现了一些问题：anyLast严重依赖数据写入顺序，业务上往往很难保证。并且需要定期的optimize table操作，才能看到更新的结果，这是很重的操作，数据量大时对资源消耗严重。

经过一段时间的研究，仔细查看了官方文档，设计总结了一套表结构，解决了上述问题。整体思路是 ReplicatedReplacingMergeTree + ReplicatedAggregatingMergeTree + MATERIALIZED VIEW来实现有限的数据更新。其中AggregatingMergeTree中使用Aggregate Function Combinators来实现数据聚合逻辑，可以实现不依赖写入顺序、按主键更新需要更新的字段、未更新字段不用写入、更新结果实时可查、最终一致的效果。

## 数据底表

有限更新，只支持按主键更新(单行更新)，Update也视为Insert，进行数据写入。更新哪个字段，写入哪个字段。

```sql
CREATE TABLE IF NOT EXISTS default.local_dat_update ON CLUSTER s2r2
(
    kafka_topic     String   COMMENT 'kafka主题',
    kafka_partition UInt16   COMMENT '所属分区',
    kafka_offset    Int64    COMMENT '分区内偏移量',
    kafka_time      DateTime COMMENT '写入kafka时间',
    kloader_time    DateTime COMMENT 'kloader处理时间',
    ------------------------------------------------------------
    pk1             UInt64     COMMENT '主键',
    pk2             String     COMMENT '主键',
    ver             DateTime64 COMMENT '版本,业务提供',
    col1            Int32      DEFAULT 0  COMMENT '选取!=0的最大版本',
    col2            String     DEFAULT '' COMMENT '选取!=空字符串的最大版本',
    col3            DateTime   DEFAULT 0  COMMENT '选取!=0的最大版本'
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/default/local_dat_update/{shard}', '{replica}')
      PARTITION BY toYYYYMM(kafka_time)
      ORDER BY (kafka_topic, kafka_partition, kafka_offset);
```

## 数据聚合表

使用组合的聚合函数实现，选取最大版本保留，与数据写入顺序无关，-If函数判断空值等情况。相同主键数据要写入同一个分区。

```sql
CREATE TABLE IF NOT EXISTS default.local_agg_update ON CLUSTER s2r2
(
    pk1     UInt64,
    pk2     String,
    version AggregateFunction(max, DateTime64),
    col1    AggregateFunction(argMaxIf, Int32, DateTime64, UInt8),
    col2    AggregateFunction(argMaxIf, String, DateTime64, UInt8),
    col3    AggregateFunction(argMaxIf, DateTime, DateTime64, UInt8)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/default/local_agg_update/{shard}', '{replica}')
      PARTITION BY (xxHash64(pk1, pk2) % 10)
      ORDER BY (pk1, pk2);
```

## 数据触发器

数据写入底表时触发，按part进行聚合，-If函数判断字段未更新的情况，argMax取最新版本的字段值。

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_update
            ON CLUSTER s2r2
            TO default.local_agg_update
AS
SELECT pk1                                  AS pk1,
       pk2                                  AS pk2,
       maxState(ver)                        AS version,
       -- 根据业务需求调整组合函数及判断逻辑
       argMaxIfState(col1, ver, col1 != 0)  AS col1,
       argMaxIfState(col2, ver, col2 != '') AS col2,
       argMaxIfState(col3, ver, col3 != 0)  AS col3
FROM default.local_dat_update
GROUP BY pk1, pk2;
```

## 聚合全局表

最终数据的查询入口，Select查询从这个表执行。

```sql
CREATE TABLE IF NOT EXISTS default.all_agg_update
    ON CLUSTER s2r2 AS default.local_agg_update
 ENGINE = Distributed('s2r2', 'default', 'local_agg_update', rand());
```

## 数据全局表

数据底表的全局表，有无均可，相当于日志查询入口。

```sql
CREATE TABLE IF NOT EXISTS default.all_dat_update
    ON CLUSTER s2r2 AS default.local_dat_update
 ENGINE = Distributed('s2r2', 'default', 'local_dat_update', kafka_offset);
```

## 测试数据写入

这里为了测试数据分布后的聚合情况，写入的是全局表，最终随机写入集群内的本地表中

```sql
-- insert
INSERT INTO default.all_dat_update(kafka_topic, kafka_partition, kafka_offset, kafka_time, kloader_time, pk1, pk2, ver, col1, col2, col3) VALUES ('develop', 0, 0, now(), now(), 1, 'wxy', now64(), 100, '100', now());
-- update col1,col2,col3
INSERT INTO default.all_dat_update(kafka_topic, kafka_partition, kafka_offset, kafka_time, kloader_time, pk1, pk2, ver, col1, col2, col3) VALUES ('develop', 0, 1, now(), now(), 1, 'wxy', now64(), 200, '200', now());
-- 最大版本的更新，先入库
INSERT INTO default.all_dat_update(kafka_topic, kafka_partition, kafka_offset, kafka_time, kloader_time, pk1, pk2, ver, col1, col2, col3) VALUES ('develop', 0, 3, now(), now(), 1, 'wxy', now64(), 300, '300', now());
-- 中间版本更新，最后入库
INSERT INTO default.all_dat_update(kafka_topic, kafka_partition, kafka_offset, kafka_time, kloader_time, pk1, pk2, ver, col1, col2, col3) VALUES ('develop', 0, 4, now(), now(), 1, 'wxy', addDays(now64(), -1), 400, '400', now());
```

## 测试查询

使用聚合函数查询，不用optimize table操作，数据写入后查询就能反映最新结果。

```sql
SELECT pk1,
       pk2,
       maxMerge(version)   AS version,
       argMaxIfMerge(col1) AS col1,
       argMaxIfMerge(col2) AS col2,
       argMaxIfMerge(col3) AS col3
FROM default.all_agg_update
GROUP BY pk1, pk2;
```

| pk1  | pk2  | version                 | col1 | col2 | col3                    |
| ---- | ---- | ----------------------- | ---- | ---- | ----------------------- |
| 1    | wxy  | 2021-04-28 10:20:00.000 | 300  | 300  | 2021-04-28 10:20:00.000 |

只更新col1字段数据

```sql
INSERT INTO default.all_dat_update(kafka_topic, kafka_partition, kafka_offset, kafka_time, kloader_time, pk1, pk2, ver, col1)
VALUES ('develop', 0, 5, now(), now(), 1, 'wxy', now64(), 500);
```

查询后结果

| pk1  | pk2  | version                 | col1 | col2 | col3                    |
| ---- | ---- | ----------------------- | ---- | ---- | ----------------------- |
| 1    | wxy  | 2021-04-28 10:21:18.000 | 500  | 300  | 2021-04-28 10:20:00.000 |

## 鸣谢

感谢ClickHouse User Group China微信群中的`何小婷`、`ClickHouse-小C`同学，对AggregatingMergeTree合并查询原理给与的指点与帮助。

## 参考资料

1.https://mp.weixin.qq.com/s/TUro4VWaOEIZo6BnYpsV6w	"ClickHouse在阿里云实时广告圈人业务最佳实践"
2.https://altinity.com/blog/2020/4/14/handling-real-time-updates-in-clickhouse	"Handling Real-Time Updates in ClickHouse"

------

> Author   ：王宣越
>
> 原文地址：https://github.com/wangxuanyue/ClickHouse_example
>
> E-mail    ：wangxuanyue7@163.com
>
> WeChat ：symbol7

