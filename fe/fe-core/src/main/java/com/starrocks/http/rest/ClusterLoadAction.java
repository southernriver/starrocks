// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/TableQueryPlanAction.java

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

package com.starrocks.http.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.journal.Journal;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.utils.RestClient;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ClusterLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ClusterLoadAction.class);

    private static final RestClient REST_CLIENT = new RestClient(5000);

    private static final String[] CPU_METRICS = new String[] {"cores",
            "user", "nice", "system", "idle", "iowait", "irq", "soft_irq", "steal", "guest", "guest_nice"};
    private static final Map<String, Integer> CPU_METRIC_INDEX_MAP = new LinkedHashMap<>(CPU_METRICS.length);

    private static final String[] SYSTEM_METRICS = new String[] {
            "cpu",
            "resource_group_cpu_limit_ratio",
            "resource_group_cpu_use_ratio",
            "process_mem_limit_bytes",
            "process_mem_bytes",
            "query_mem_limit_bytes",
            "query_mem_bytes"};
    private static final Map<String, Integer> METRICS_INDEX_MAP = new LinkedHashMap<>();

    static {
        for (int i = 0; i < CPU_METRICS.length; i++) {
            CPU_METRIC_INDEX_MAP.put(CPU_METRICS[i], i);
        }

        for (int i = 0; i < SYSTEM_METRICS.length; i++) {
            METRICS_INDEX_MAP.put(SYSTEM_METRICS[i], i);
        }
    }

    public ClusterLoadAction(ActionController controller) {
        super(controller);
    }

    static class TrackerInfo {
        private boolean ok = false;

        private long collectTime = 0;
        private long lastCollectTime = 0;

        // cpu
        private long[] cpuMetrics = new long[CPU_METRICS.length];
        private long[] lastCpuMetrics = new long[CPU_METRICS.length];

        private double resourceGroupCpuLimitRatio = 1.0;
        private double resourceGroupCpuUseRatio = 0;

        // mem
        private long processMemLimitBytes = -1;
        private long processMemBytes = 0;
        private long queryMemLimitBytes = -1;
        private long queryMemBytes = 0;

        public TrackerInfo() {
        }

        public TrackerInfo(long collectTime) {
            this.collectTime = collectTime;
        }

        public TrackerInfo updateLastCpuMetrics(TrackerInfo lastTrackerInfo) {
            for (int i = 0; i < this.lastCpuMetrics.length; i++) {
                this.lastCpuMetrics[i] = lastTrackerInfo.getCpuMetrics()[i];
            }
            this.lastCollectTime = lastTrackerInfo.getCollectTime();
            return this;
        }

        public TrackerInfo load(JsonNode metric) {
            JsonNode metricName = metric.get("tags").get("metric");
            Integer index = METRICS_INDEX_MAP.get(metricName.asText());
            if (null == index) {
                return this;
            }
            JsonNode metricValue = metric.get("value");
            JsonNode metricUnit = metric.get("unit");
            switch (SYSTEM_METRICS[index]) {
                case "cpu":
                    Integer modeIndex = CPU_METRIC_INDEX_MAP.get(metric.get("tags").get("mode").asText());
                    if (null == modeIndex) {
                        return this;
                    }
                    this.cpuMetrics[modeIndex] = metricValue.asLong();
                    break;
                case "resource_group_cpu_limit_ratio":
                    this.resourceGroupCpuLimitRatio = metricValue.asDouble();
                    break;
                case "resource_group_cpu_use_ratio":
                    this.resourceGroupCpuUseRatio = metricValue.asDouble();
                    break;
                case "process_mem_limit_bytes":
                    this.processMemLimitBytes = metricValue.asLong();
                    break;
                case "process_mem_bytes":
                    this.processMemBytes = metricValue.asLong();
                    break;
                case "query_mem_limit_bytes":
                    this.queryMemLimitBytes = metricValue.asLong();
                    break;
                case "query_mem_bytes":
                    this.queryMemBytes = metricValue.asLong();
                    break;
                default:
                    break;
            }

            return this;
        }

        public long[] getCpuMetrics() {
            return cpuMetrics;
        }

        public void setCpuMetrics(long[] cpuMetrics) {
            this.cpuMetrics = cpuMetrics;
        }

        public long[] getLastCpuMetrics() {
            return lastCpuMetrics;
        }

        public void setLastCpuMetrics(long[] lastCpuMetrics) {
            this.lastCpuMetrics = lastCpuMetrics;
        }

        public double getResourceGroupCpuLimitRatio() {
            return resourceGroupCpuLimitRatio;
        }

        public void setResourceGroupCpuLimitRatio(double resourceGroupCpuLimitRatio) {
            this.resourceGroupCpuLimitRatio = resourceGroupCpuLimitRatio;
        }

        public double getResourceGroupCpuUseRatio() {
            return resourceGroupCpuUseRatio;
        }

        public void setResourceGroupCpuUseRatio(double resourceGroupCpuUseRatio) {
            this.resourceGroupCpuUseRatio = resourceGroupCpuUseRatio;
        }

        public long getProcessMemLimitBytes() {
            return processMemLimitBytes;
        }

        public void setProcessMemLimitBytes(long processMemLimitBytes) {
            this.processMemLimitBytes = processMemLimitBytes;
        }

        public long getProcessMemBytes() {
            return processMemBytes;
        }

        public void setProcessMemBytes(long processMemBytes) {
            this.processMemBytes = processMemBytes;
        }

        public long getQueryMemLimitBytes() {
            return queryMemLimitBytes;
        }

        public void setQueryMemLimitBytes(long queryMemLimitBytes) {
            this.queryMemLimitBytes = queryMemLimitBytes;
        }

        public long getQueryMemBytes() {
            return queryMemBytes;
        }

        public void setQueryMemBytes(long queryMemBytes) {
            this.queryMemBytes = queryMemBytes;
        }

        public boolean isOk() {
            return ok;
        }

        public void setOk(boolean ok) {
            this.ok = ok;
        }

        public long getCollectTime() {
            return collectTime;
        }

        public void setCollectTime(long collectTime) {
            this.collectTime = collectTime;
        }

        public long getLastCollectTime() {
            return lastCollectTime;
        }

        public void setLastCollectTime(long lastCollectTime) {
            this.lastCollectTime = lastCollectTime;
        }
    }

    public static class MonitorMgr extends LeaderDaemon {
        private final ExecutorService executor;

        public MonitorMgr() {
            super("monitor mgr", Config.monitor_timeout_second * 1000L);
            this.executor = ThreadPoolManager.newDaemonFixedThreadPool(Config.heartbeat_mgr_threads_num,
                    Config.heartbeat_mgr_blocking_queue_size, "monitor-mgr-pool", false);
        }

        @Override
        protected void runAfterCatalogReady() {
            List<Future<TrackerInfo>> trackerInfoList = Lists.newArrayList();

            final long currentTime = System.currentTimeMillis();

            // send backend monitor
            for (Backend backend : GlobalStateMgr.getCurrentSystemInfo().getIdToBackend().values()) {
                if (!backend.isAvailable()) {
                    //skip
                    continue;
                }

                BackendMonitorHandler handler = new BackendMonitorHandler(backend, currentTime);
                trackerInfoList.add(executor.submit(handler));
            }

            // send backend monitor
            for (ComputeNode computeNode : GlobalStateMgr.getCurrentSystemInfo().getIdComputeNode().values()) {
                if (!computeNode.isAvailable()) {
                    //skip
                    continue;
                }

                BackendMonitorHandler handler = new BackendMonitorHandler(computeNode, currentTime);
                trackerInfoList.add(executor.submit(handler));
            }

            for (Future<TrackerInfo> future : trackerInfoList) {
                try {
                    TrackerInfo trackerInfo = future.get();
                    if (!trackerInfo.isOk()) {
                        LOG.warn("get bad tracker info response: {}", trackerInfo);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOG.warn("got exception when doing heartbeat", e);
                    continue;
                }
            } // end for all results
        }
    }

    // backend heartbeat
    static class BackendMonitorHandler implements Callable<TrackerInfo> {
        private final ComputeNode computeNode;
        private final long currentTime;

        private final ObjectMapper objectMapper;

        public BackendMonitorHandler(ComputeNode computeNode, long currentTime) {
            this.computeNode = computeNode;
            this.currentTime = currentTime;
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public TrackerInfo call() throws Exception {
            TrackerInfo newTrackerInfo = new TrackerInfo(currentTime);
            String metricsUrl =
                    "http://" + computeNode.getHost() + ":" + computeNode.getHttpPort() + "/metrics?type=json";
            try {
                String metricsStr = REST_CLIENT.httpGet(metricsUrl);
                JsonNode json = objectMapper.readTree(metricsStr);
                if (json.isArray()) {
                    for (JsonNode metric : json) {
                        newTrackerInfo.load(metric);
                    }
                }
                newTrackerInfo.setOk(true);
            } catch (IOException e) {
                LOG.error("Failed to get backend: {} metrics info", computeNode, e);
            }

            try {
                DatabaseEntry theKey = new DatabaseEntry();
                TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
                idBinding.objectToEntry(computeNode.getId(), theKey);

                DatabaseEntry theData = new DatabaseEntry();
                OperationStatus readStatus =
                        Objects.requireNonNull(getMonitorDB()).get(null, theKey, theData, LockMode.DEFAULT);
                if (readStatus == OperationStatus.SUCCESS) {
                    TrackerInfo lastTrackerInfo = objectMapper.readValue(theData.getData(), TrackerInfo.class);
                    if (currentTime - lastTrackerInfo.getCollectTime() < 60_000) {
                        newTrackerInfo.updateLastCpuMetrics(lastTrackerInfo);
                    }
                }

                theData = new DatabaseEntry(objectMapper.writeValueAsBytes(newTrackerInfo));
                OperationStatus writeStatus = Objects.requireNonNull(getMonitorDB()).put(null, theKey, theData);
                if (writeStatus != OperationStatus.SUCCESS) {
                    LOG.warn("Failed to save data to monitor db!");
                }
            } catch (Exception e) {
                LOG.error("Failed to save data to monitor db", e);
            }

            return newTrackerInfo;
        }
    }

    public static CloseSafeDatabase getMonitorDB() {
        Journal journal = GlobalStateMgr.getCurrentState().getJournal();
        if (journal instanceof BDBJEJournal) {
            BDBEnvironment bdbEnvironment = ((BDBJEJournal) journal).getBdbEnvironment();
            if (bdbEnvironment != null) {
                return bdbEnvironment.getMonitorDB();
            }
        }
        return null;
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/cluster_load", new ClusterLoadAction(controller));
    }

    private TrackerInfo getTrackerInfoFromMeta(ComputeNode computeNode) {
        TrackerInfo lastTrackerInfo = null;
        try {
            DatabaseEntry theKey = new DatabaseEntry();
            TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
            idBinding.objectToEntry(computeNode.getId(), theKey);

            DatabaseEntry theData = new DatabaseEntry();
            OperationStatus readStatus =
                    Objects.requireNonNull(getMonitorDB()).get(null, theKey, theData, LockMode.DEFAULT);
            if (readStatus == OperationStatus.SUCCESS) {
                lastTrackerInfo = new ObjectMapper().readValue(theData.getData(), TrackerInfo.class);
            }
        } catch (Exception e) {
            LOG.error("Failed to read data from monitor db", e);
        }
        return lastTrackerInfo;
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        // send backend monitor
        ClusterWorkloadBean workload = new ClusterWorkloadBean();
        workload.setEngine("STARROCKS");
        long currentTime = System.currentTimeMillis();

        long activeWorkers = 0L;
        workload.setTotal(new ClusterWorkloadBean.ResourceBean());
        workload.setUsed(new ClusterWorkloadBean.ResourceBean());
        workload.setAvailable(new ClusterWorkloadBean.ResourceBean());
        workload.setMaxResourcePerNode(new ClusterWorkloadBean.ResourceBean());

        double avgProcessCpuUtilization = 0.0;
        double avgSystemCpuUtilization = 0.0;
        double avgHeapUtilization = 0.0;
        List<ComputeNode> computeNodeList = Lists.newArrayList();
        if ("cn".equalsIgnoreCase(request.getSingleParameter("type"))) {
            computeNodeList.addAll(GlobalStateMgr.getCurrentSystemInfo().getIdComputeNode().values());
        } else {
            computeNodeList.addAll(GlobalStateMgr.getCurrentSystemInfo().getIdToBackend().values());
        }
        for (ComputeNode computeNode : computeNodeList) {
            if (!computeNode.isAvailable()) {
                //skip
                continue;
            }

            TrackerInfo lastTrackerInfo = getTrackerInfoFromMeta(computeNode);
            if (null == lastTrackerInfo
                    || currentTime - lastTrackerInfo.getCollectTime() > 60_000) {
                continue;
            }

            activeWorkers++;

            final long memUsed = lastTrackerInfo.getProcessMemBytes();
            final long memLimit = lastTrackerInfo.getProcessMemLimitBytes();
            workload.getTotal().addMemory(memLimit);
            workload.getUsed().addMemory(memUsed);
            workload.getAvailable().addMemory(memLimit - memUsed);
            avgHeapUtilization += 1.0 * memUsed / memLimit;

            final long cpuCores = lastTrackerInfo.getCpuMetrics()[0];
            workload.getTotal().addCpu(cpuCores);
            if (workload.getMaxResourcePerNode().getCpu() < cpuCores) {
                workload.getMaxResourcePerNode().setCpu(cpuCores);
                workload.getMaxResourcePerNode().setMemory(memLimit);
            }

            Pair<Double, Double> cpuUtilization = getProcessCpuUtilization(lastTrackerInfo);
            avgProcessCpuUtilization += cpuUtilization.getKey();
            avgSystemCpuUtilization += cpuUtilization.getValue();
        }

        if (activeWorkers < 1) {
            workload.setActiveWorkers(0L);
            workload.setOverload(true);
            sendResultByJson(request, response, workload);
            return;
        }

        workload.setAvgProcessCpuUtilization(avgProcessCpuUtilization / activeWorkers);
        workload.setAvgSystemCpuUtilization(avgSystemCpuUtilization / activeWorkers);
        workload.setAvgHeapUtilization(avgHeapUtilization / activeWorkers);
        workload.setActiveWorkers(activeWorkers);

        workload.setOverload(isOverLoad(workload, computeNodeList.size()));
        sendResultByJson(request, response, workload);
    }

    private long getCpuTime(long[] cpuMetrics, int from, int end) {
        long sum = 0;
        for (int i = from; i <= end; i++) {
            sum += cpuMetrics[i];
        }
        return sum;
    }

    private Pair<Double, Double> getProcessCpuUtilization(TrackerInfo lastTrackerInfo) {
        long duration = lastTrackerInfo.getCollectTime() - lastTrackerInfo.getLastCollectTime();
        if (duration > 1_000 && duration < 60_000) {
            long totalCpuTime = getCpuTime(lastTrackerInfo.getCpuMetrics(), 1, 7);
            long lastTotalCpuTime = getCpuTime(lastTrackerInfo.getLastCpuMetrics(), 1, 7);

            long useCpuTime = getCpuTime(lastTrackerInfo.getCpuMetrics(), 1, 3);
            long lastUseCpuTime = getCpuTime(lastTrackerInfo.getLastCpuMetrics(), 1, 3);

            long systemCpuTime = getCpuTime(lastTrackerInfo.getCpuMetrics(), 3, 3);
            long lastSystemCpuTime = getCpuTime(lastTrackerInfo.getLastCpuMetrics(), 3, 3);

            long systemCost = systemCpuTime - lastSystemCpuTime;
            long useCost = useCpuTime - lastUseCpuTime;
            long totalCost = totalCpuTime - lastTotalCpuTime;
            if (totalCost > 0) {
                return Pair.of(1.0 * useCost / totalCost * 100, 1.0 * systemCost / totalCost * 100);
            }
        }
        return Pair.of(0.0, 0.0);
    }

    private boolean isOverLoad(ClusterWorkloadBean bean, long allWorkers) {
        return (1.0 * bean.getActiveWorkers() / allWorkers) < 0.5
                // && bean.getActiveWorkers() < 10
                || bean.getAvgProcessCpuUtilization() > Config.cluster_overload_cpu_rate * 100
                || bean.getAvgHeapUtilization() > Config.cluster_overload_mem_rate;
    }

    static class ClusterWorkloadBean {
        private String engine;
        private ResourceBean total;
        private ResourceBean available;
        private ResourceBean used;
        private Boolean overload;
        private long activeWorkers;
        private Double avgProcessCpuUtilization;
        private Double avgSystemCpuUtilization;
        private Double avgHeapUtilization;
        private ResourceBean maxResourcePerNode;

        public String getEngine() {
            return engine;
        }

        public void setEngine(String engine) {
            this.engine = engine;
        }

        public ResourceBean getTotal() {
            return total;
        }

        public void setTotal(ResourceBean total) {
            this.total = total;
        }

        public ResourceBean getAvailable() {
            return available;
        }

        public void setAvailable(ResourceBean available) {
            this.available = available;
        }

        public ResourceBean getUsed() {
            return used;
        }

        public void setUsed(ResourceBean used) {
            this.used = used;
        }

        public Boolean getOverload() {
            return overload;
        }

        public void setOverload(Boolean overload) {
            this.overload = overload;
        }

        public Long getActiveWorkers() {
            return activeWorkers;
        }

        public void setActiveWorkers(Long activeWorkers) {
            this.activeWorkers = activeWorkers;
        }

        public Double getAvgProcessCpuUtilization() {
            return avgProcessCpuUtilization;
        }

        public void setAvgProcessCpuUtilization(Double avgProcessCpuUtilization) {
            this.avgProcessCpuUtilization = avgProcessCpuUtilization;
        }

        public Double getAvgSystemCpuUtilization() {
            return avgSystemCpuUtilization;
        }

        public void setAvgSystemCpuUtilization(Double avgSystemCpuUtilization) {
            this.avgSystemCpuUtilization = avgSystemCpuUtilization;
        }

        public Double getAvgHeapUtilization() {
            return avgHeapUtilization;
        }

        public void setAvgHeapUtilization(Double avgHeapUtilization) {
            this.avgHeapUtilization = avgHeapUtilization;
        }

        public ResourceBean getMaxResourcePerNode() {
            return maxResourcePerNode;
        }

        public void setMaxResourcePerNode(ResourceBean maxResourcePerNode) {
            this.maxResourcePerNode = maxResourcePerNode;
        }

        /**
         * Resource field in Response
         */
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class ResourceBean {
            private long cpu;
            private long memory;

            public long getCpu() {
                return cpu;
            }

            public void setCpu(long cpu) {
                this.cpu = cpu;
            }

            public void addCpu(long cpu) {
                this.cpu += cpu;
            }

            public long getMemory() {
                return memory;
            }

            public void setMemory(long memory) {
                this.memory = memory;
            }

            public void addMemory(long memory) {
                this.memory += memory;
            }
        }
    }
}
