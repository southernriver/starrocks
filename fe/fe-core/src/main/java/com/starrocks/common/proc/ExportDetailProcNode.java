// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ExportProcNode.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.qe.Coordinator;
import com.starrocks.thrift.TUniqueId;
import org.joda.time.format.ISODateTimeFormat;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ExportDetailProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Be").add("ErrorCode").add("ErrorMsg").add("IsDone")
            .add("IsCanceled").add("startTime").add("finishTime").add("Seconds")
            .build();

    private final ExportMgr exportMgr;
    private final String jobId;

    public ExportDetailProcNode(ExportMgr exportMgr, String jobId) {
        this.exportMgr = exportMgr;
        this.jobId = jobId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(exportMgr);
        Preconditions.checkNotNull(jobId);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ExportJob job = exportMgr.getExportJob(Long.parseLong(jobId));
        if (job == null) {
            return result;
        }
        Map<TUniqueId, List<Coordinator.BackendExecResult>> taskDetails = job.getBackendTaskExecResult();
        if (taskDetails == null) {
            return result;
        }
        LinkedList<List<String>> details = new LinkedList<>();
        for (List<Coordinator.BackendExecResult> list : taskDetails.values()) {
            for (Coordinator.BackendExecResult r : list) {
                List<String> taskInfo = Lists.newArrayListWithExpectedSize(TITLE_NAMES.size());
                taskInfo.add(r.getAddress().getHostname() + ":" + r.getAddress().getPort());
                taskInfo.add(r.getCode() != null ? r.getCode().name() : "");
                taskInfo.add(r.getErrMsg() != null ? r.getErrMsg() : "");
                taskInfo.add(String.valueOf(r.isDone()));
                taskInfo.add(String.valueOf(r.isHasCanceled()));
                taskInfo.add(ISODateTimeFormat.dateTime().print(r.getStartTime()));
                taskInfo.add(r.getFinishTime() > 0 ? ISODateTimeFormat.dateTime().print(r.getFinishTime()) : "");
                taskInfo.add(String.valueOf(r.getCost()));
                details.add(taskInfo);
            }
        }
        result.setRows(details);
        return result;
    }
}
