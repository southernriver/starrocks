// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.export;

public enum ExportTargetType {
    HDFS,
    EXTERNAL_TABLE,
    HIVE,
    ICEBERG,
    HUDI,
    LOCAL
}
