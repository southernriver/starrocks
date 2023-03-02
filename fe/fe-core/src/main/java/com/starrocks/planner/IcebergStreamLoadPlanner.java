// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/StreamLoadPlanner.java

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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.load.routineload.IcebergSplit;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.thrift.TUniqueId;

import java.util.List;

public class IcebergStreamLoadPlanner extends StreamLoadPlanner {
    private final BrokerDesc brokerDesc;
    private final long beId;
    private final List<IcebergSplit> splits;

    public IcebergStreamLoadPlanner(Database db, OlapTable destTable, StreamLoadInfo streamLoadInfo,
                                    BrokerDesc brokerDesc, long beId, List<IcebergSplit> splits) {
        super(db, destTable, streamLoadInfo);
        this.brokerDesc = brokerDesc;
        this.beId = beId;
        this.splits = splits;
    }

    @Override
    protected ScanNode createScanNode(TUniqueId loadId, TupleDescriptor tupleDesc)
            throws UserException {
        IcebergStreamLoadScanNode scanNode =
                new IcebergStreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable, streamLoadInfo,
                        brokerDesc, beId, splits);
        scanNode.setUseVectorizedLoad(true);
        scanNode.init(analyzer);
        scanNode.finalizeStats(analyzer);
        return scanNode;
    }
}
