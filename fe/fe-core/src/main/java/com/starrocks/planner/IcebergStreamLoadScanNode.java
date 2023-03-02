// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/StreamLoadScanNode.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.Table;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.load.routineload.IcebergSplit;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;

import java.util.List;

/**
 * used to scan from iceberg
 */
public class IcebergStreamLoadScanNode extends StreamLoadScanNode {
    private final BrokerDesc brokerDesc;
    private final long beId;
    private final List<IcebergSplit> splits;

    public IcebergStreamLoadScanNode(
            TUniqueId loadId, PlanNodeId id, TupleDescriptor tupleDesc, Table dstTable, StreamLoadInfo streamLoadInfo,
            BrokerDesc brokerDesc, long beId, List<IcebergSplit> splits) {
        super(loadId, id, tupleDesc, dstTable, streamLoadInfo);
        this.brokerDesc = brokerDesc;
        this.beId = beId;
        this.splits = splits;
    }

    // private TBrokerRangeDesc createBrokerRangeDesc(IcebergSplit split) {
    //     TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
    //     rangeDesc.setFile_type(TFileType.FILE_BROKER);
    //     rangeDesc.setFormat_type(splits.get(0).getFormatType());
    //     rangeDesc.setPath(split.getPath());
    //     rangeDesc.setSplittable(false);
    //     rangeDesc.setStart_offset(split.getOffset());
    //     rangeDesc.setSize(split.getLength());
    //     rangeDesc.setFile_size(split.getFileSize());
    //     rangeDesc.setNum_of_columns_from_file(srcTupleDesc.getSlots().size());
    //     rangeDesc.setLoad_id(loadId);
    //     return rangeDesc;
    // }
}
