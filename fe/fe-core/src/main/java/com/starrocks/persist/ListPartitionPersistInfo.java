// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.ReplicaAssignment;

import java.util.List;

public class ListPartitionPersistInfo extends PartitionPersistInfoV2 {

    @SerializedName("values")
    private List<String> values;
    @SerializedName("multiValues")
    private List<List<String>> multiValues;

    public ListPartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                    DataProperty dataProperty, ReplicaAssignment replicaAssignment,
                                    boolean isInMemory, boolean isTempPartition,
                                    List<String> values, List<List<String>> multiValues) {
        super(dbId, tableId, partition, dataProperty, replicaAssignment, isInMemory, isTempPartition);
        this.multiValues = multiValues;
        this.values = values;
    }

    public List<String> getValues() {
        return values;
    }

    public List<List<String>> getMultiValues() {
        return multiValues;
    }

}