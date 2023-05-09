// This file is made available under Elastic License 2.0.

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

package com.starrocks.load;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ColddownScheduler extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(ColddownScheduler.class);

    private final ColddownMgr colddownMgr;
    private int startTime;
    private int endTime;

    @VisibleForTesting
    public ColddownScheduler() {
        super();
        colddownMgr = GlobalStateMgr.getCurrentState().getColddownMgr();
    }

    public ColddownScheduler(ColddownMgr colddownMgr) {
        super("Colddown scheduler", Config.colddown_scheduler_interval_second * 1000);
        this.colddownMgr = colddownMgr;
        if (!initWorkTime()) {
            LOG.error("failed to init time in ColddownScheduler. exit");
            System.exit(-1);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of ColddownScheduler", e);
        }
    }

    private void process() {
        // get need schedule colddown jobs
        List<ColddownJob> colddownJobList = getNeedScheduleColddownJobs();
        for (ColddownJob colddownJob : colddownJobList) {
            colddownJob.handleExportDone();
        }
        if (!itsTime()) {
            return;
        }

        if (!colddownJobList.isEmpty()) {
            LOG.info("there are {} job(s) need to be scheduled", colddownJobList.size());
        }
        for (ColddownJob colddownJob : colddownJobList) {
            try {
                if (!colddownJob.preCheck()) {
                    // FIXME: how this LogKey works?
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, colddownJob.getId())
                            .add("msg", "preCheck fail")
                            .build());
                    continue;
                }
                colddownJob.startCheck();
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }
    }

    private boolean initWorkTime() {
        Date startDate = TimeUtils.getTimeAsDate(Config.cold_down_check_start_time);
        Date endDate = TimeUtils.getTimeAsDate(Config.cold_down_check_end_time);

        if (startDate == null || endDate == null) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(startDate);
        startTime = calendar.get(Calendar.HOUR_OF_DAY);

        calendar.setTime(endDate);
        endTime = calendar.get(Calendar.HOUR_OF_DAY);
        return true;
    }

    /*
     * check if it's time to do colddown
     */
    private boolean itsTime() {
        if (startTime == endTime) {
            return false;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        int currentTime = calendar.get(Calendar.HOUR_OF_DAY);

        boolean isTime = false;
        if (startTime < endTime) {
            isTime = currentTime >= startTime && currentTime <= endTime;
        } else {
            // startTime > endTime (across the day)
            isTime = currentTime >= startTime || currentTime <= endTime;
        }

        if (!isTime) {
            LOG.debug("current time is {}:00, waiting to {}:00 to {}:00",
                    currentTime, startTime, endTime);
        }

        return isTime;
    }

    private List<ColddownJob> getNeedScheduleColddownJobs() {
        return colddownMgr.getColddownJobs(ColddownJob.JobState.RUNNING);
    }
}
