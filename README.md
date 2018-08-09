# CallLogSytem
利用 HDFS、HBase、Flume、ZooKeeper 实现通话日志数据的存储，随机访问与实时读写。通过 hash 技术对 rowkey 进行 分析处理，解决 HBase 热点问题，协同 coprocessor，结合 Hive 查询，将结果展示在出来。
