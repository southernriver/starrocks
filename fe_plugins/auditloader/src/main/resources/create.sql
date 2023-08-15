CREATE TABLE `starrocks_audit_tbl` (
    `day` int NULL COMMENT "分区字段，天分区",
    `hour` int NULL COMMENT "小时",
    `cluster_name` varchar(48) NULL COMMENT "集群名",
    `query_id` varchar(48) NULL COMMENT "查询唯一ID",
    `time` datetime NOT NULL COMMENT "查询开始时间",
    `client_ip` varchar(32) NULL COMMENT "客户端IP:端口",
    `user` varchar(64) NULL COMMENT "查询用户名",
    `resource_group` varchar(100) NULL COMMENT "",
    `catalog` varchar(100) NULL COMMENT "",
    `db` varchar(96) NULL COMMENT "查询所在数据库,多个库用逗号分隔",
    `query_table` varchar(512) NULL COMMENT "查询的表，多张表用逗号分隔",
    `state` varchar(8) NULL COMMENT "查询状态：EOF，ERR，OK",
    `error_code` varchar(20) NULL COMMENT "错误码",
    `query_time` bigint(20) NULL COMMENT "查询执行时间（毫秒）",
    `scan_bytes` bigint(20) NULL COMMENT "查询扫描的字节数",
    `scan_rows` bigint(20) NULL COMMENT "查询扫描的记录行数",
    `return_rows` bigint(20) NULL COMMENT "查询返回的结果行数",
    `cpu_cost_ns` bigint(20) NULL COMMENT "查询消耗的cpu",
    `mem_cost_bytes` bigint(20) NULL COMMENT "查询消耗的内存",
    `stmt_id` int(11) NULL COMMENT "SQL语句的增量ID",
    `is_query` tinyint(4) NULL COMMENT "SQL是否为查询. 1或0",
    `data_source` varchar(8) NULL COMMENT "查询数据源，OLAP、lake、hybird",
    `request_type` varchar(10) NULL COMMENT "查询类型：INSERT、SELECT、SET等",
    `frontend_ip` varchar(32) NULL COMMENT "执行该语句的FE IP",
    `stmt` varchar(1048576) NULL COMMENT "SQL",
    `digest` varchar(32) NULL COMMENT "SQL指纹",
    `plan_cpu_costs` bigint(20) NULL COMMENT "",
    `plan_mem_costs` bigint(20) NULL COMMENT "",
    `exception` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`query_id`, `time`, `client_ip`)
COMMENT "OLAP"
PARTITION BY RANGE(`day`)
(PARTITION p_20230815 VALUES [("2023-08-15"), ("2023-08-16")))
DISTRIBUTED BY HASH(`query_id`) BUCKETS 20
PROPERTIES (
"replication_num" = "3",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"dynamic_partition.start" = "-30",
"dynamic_partition.end" = "2",
"dynamic_partition.prefix" = "p_",
"dynamic_partition.buckets" = "3",
"dynamic_partition.history_partition_num" = "2",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);