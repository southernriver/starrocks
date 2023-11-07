// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ResourceGroup implements Writable {
    public static final String GROUP_TYPE = "type";
    public static final String USER = "user";
    public static final String ROLE = "role";
    public static final String QUERY_TYPE = "query_type";
    public static final String SOURCE_IP = "source_ip";
    public static final String DATABASES = "db";
    public static final String CPU_CORE_LIMIT = "cpu_core_limit";
    public static final String MEM_LIMIT = "mem_limit";
    public static final String BIG_QUERY_MEM_LIMIT = "big_query_mem_limit";
    public static final String BIG_QUERY_SCAN_ROWS_LIMIT = "big_query_scan_rows_limit";
    public static final String BIG_QUERY_CPU_SECOND_LIMIT = "big_query_cpu_second_limit";
    public static final String CONCURRENCY_LIMIT = "concurrency_limit";
    public static final String DEFAULT_RESOURCE_GROUP_NAME = "default_wg";
    public static final String CN_NUMBER = "cn.number";
    public static final String MAX_CN_NUMBER = "cn.number.max";
    public static final String BE_NUMBER = "be.number";
    public static final String MAX_BE_NUMBER = "be.number.max";

    public static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(100)))
                    .addColumn(new Column("id", ScalarType.createVarchar(200)))
                    .addColumn(new Column("cpu_core_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_cpu_second_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_scan_rows_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("big_query_mem_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("concurrency_limit", ScalarType.createVarchar(200)))
                    .addColumn(new Column("type", ScalarType.createVarchar(200)))
                    .addColumn(new Column("classifiers", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("cn_number", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("max_cn_number", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("be_number", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("max_be_number", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("cn_list", ScalarType.createVarchar(10240)))
                    .addColumn(new Column("be_list", ScalarType.createVarchar(10240)))
                    .build();
    @SerializedName(value = "classifiers")
    List<ResourceGroupClassifier> classifiers = Lists.newArrayList();
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "cpuCoreLimit")
    private Integer cpuCoreLimit;
    @SerializedName(value = "memLimit")
    private Double memLimit;
    @SerializedName(value = "bigQueryMemLimit")
    private Long bigQueryMemLimit;
    @SerializedName(value = "bigQueryScanRowsLimit")
    private Long bigQueryScanRowsLimit;
    @SerializedName(value = "bigQueryCpuSecondLimit")
    private Long bigQueryCpuSecondLimit;
    @SerializedName(value = "concurrencyLimit")
    private Integer concurrencyLimit;
    @SerializedName(value = "workGroupType")
    private TWorkGroupType resourceGroupType;
    @SerializedName(value = "version")
    private long version;
    @SerializedName(value = "cnNumber")
    private Integer cnNumber;
    @SerializedName(value = "maxCnNumber")
    private Integer maxCnNumber;
    @SerializedName(value = "beNumber")
    private Integer beNumber;
    @SerializedName(value = "maxBeNumber")
    private Integer maxBeNumber;
    @SerializedName(value = "cnList")
    private List<Long> cnList = Lists.newArrayList();
    @SerializedName(value = "beList")
    private List<Long> beList = Lists.newArrayList();


    public ResourceGroup() {
    }

    public static ResourceGroup read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceGroup.class);
    }

    private List<String> showClassifier(ResourceGroupClassifier classifier) {
        List<String> row = new ArrayList<>();
        row.add(this.name);
        row.add("" + this.id);
        if (cpuCoreLimit != null) {
            row.add("" + cpuCoreLimit);
        } else {
            row.add("" + 0);
        }
        if (memLimit != null) {
            row.add("" + (memLimit * 100) + "%");
        } else {
            row.add("" + 0);
        }
        if (bigQueryCpuSecondLimit != null) {
            row.add("" + bigQueryCpuSecondLimit);
        } else {
            row.add("" + 0);
        }
        if (bigQueryScanRowsLimit != null) {
            row.add("" + bigQueryScanRowsLimit);
        } else {
            row.add("" + 0);
        }
        if (bigQueryMemLimit != null) {
            row.add("" + bigQueryMemLimit);
        } else {
            row.add("" + 0);
        }
        if (concurrencyLimit != null) {
            row.add("" + concurrencyLimit);
        } else {
            row.add("" + 0);
        }
        row.add("" + resourceGroupType.name().substring("WG_".length()));
        row.add(classifier.toString());
        row.add("" + Optional.ofNullable(getCnNumber()).orElse(0));
        row.add("" + Optional.ofNullable(getMaxCnNumber()).orElse(0));
        row.add("" + Optional.ofNullable(getBeNumber()).orElse(0));
        row.add("" + Optional.ofNullable(getMaxBeNumber()).orElse(0));
        row.add(Arrays.toString(getCnList().toArray()));
        row.add(Arrays.toString(getBeList().toArray()));
        return row;
    }

    public List<List<String>> showVisible(String user, String roleName, String ip) {
        return classifiers.stream().filter(c -> c.isVisible(user, roleName, ip))
                .map(this::showClassifier).collect(Collectors.toList());
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public List<List<String>> show() {
        if (classifiers.isEmpty()) {
            return Collections.singletonList(showClassifier(new ResourceGroupClassifier()));
        }
        return classifiers.stream().map(this::showClassifier).collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public TWorkGroup toThrift() {
        TWorkGroup twg = new TWorkGroup();
        twg.setName(name);
        twg.setId(id);
        if (cpuCoreLimit != null) {
            twg.setCpu_core_limit(cpuCoreLimit);
        }
        if (memLimit != null) {
            twg.setMem_limit(memLimit);
        }

        if (bigQueryMemLimit != null) {
            twg.setBig_query_mem_limit(bigQueryMemLimit);
        }

        if (bigQueryScanRowsLimit != null) {
            twg.setBig_query_scan_rows_limit(bigQueryScanRowsLimit);
        }

        if (bigQueryCpuSecondLimit != null) {
            twg.setBig_query_cpu_second_limit(bigQueryCpuSecondLimit);
        }

        if (concurrencyLimit != null) {
            twg.setConcurrency_limit(concurrencyLimit);
        }
        if (resourceGroupType != null) {
            twg.setWorkgroup_type(resourceGroupType);
        }
        twg.setVersion(version);
        return twg;
    }

    public Integer getCpuCoreLimit() {
        return cpuCoreLimit;
    }

    public void setCpuCoreLimit(int cpuCoreLimit) {
        this.cpuCoreLimit = cpuCoreLimit;
    }

    public Double getMemLimit() {
        return memLimit;
    }

    public void setMemLimit(double memLimit) {
        this.memLimit = memLimit;
    }

    public Long getBigQueryMemLimit() {
        return bigQueryMemLimit;
    }

    public void setBigQueryMemLimit(long limit) {
        bigQueryMemLimit = limit;
    }

    public Long getBigQueryScanRowsLimit() {
        return bigQueryScanRowsLimit;
    }

    public void setBigQueryScanRowsLimit(long limit) {
        bigQueryScanRowsLimit = limit;
    }

    public Long getBigQueryCpuSecondLimit() {
        return bigQueryCpuSecondLimit;
    }

    public void setBigQueryCpuSecondLimit(long limit) {
        bigQueryCpuSecondLimit = limit;
    }

    public Integer getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public TWorkGroupType getResourceGroupType() {
        return resourceGroupType;
    }

    public void setResourceGroupType(TWorkGroupType workGroupType) {
        this.resourceGroupType = workGroupType;
    }

    public List<ResourceGroupClassifier> getClassifiers() {
        return classifiers;
    }

    public void setClassifiers(List<ResourceGroupClassifier> classifiers) {
        this.classifiers = classifiers;
    }

    public Integer getCnNumber() {
        return cnNumber;
    }

    public void setCnNumber(int cnNumber) {
        this.cnNumber = cnNumber;
    }

    public Integer getMaxCnNumber() {
        return maxCnNumber;
    }

    public void setMaxCnNumber(int maxCnNumber) {
        this.maxCnNumber = maxCnNumber;
    }

    public Integer getBeNumber() {
        return beNumber;
    }

    public void setBeNumber(int beNumber) {
        this.beNumber = beNumber;
    }

    public Integer getMaxBeNumber() {
        return maxBeNumber;
    }

    public void setMaxBeNumber(int maxBeNumber) {
        this.maxBeNumber = maxBeNumber;
    }

    public List<Long> getCnList() {
        return cnList;
    }

    public void setCnList(List<Long> cnList) {
        this.cnList = ImmutableList.<Long>copyOf(cnList);
    }

    public List<Long> getBeList() {
        return beList;
    }

    public void addBackend(Long id) {
        if (GlobalStateMgr.getCurrentSystemInfo().getBackend(id) == null) {
            throw new IllegalArgumentException("Backend id " + id + " does not exist!");
        }
        if (beList.contains(id)) {
            throw new IllegalArgumentException("Backend  " + id + ", already added!");
        }
        if (beList.size() + 1 > maxBeNumber) {
            throw new IllegalArgumentException("Backend number exceed limit " + maxBeNumber
                    + ", please alter `be.number.max` first");
        }
        List<Long> list = Lists.newArrayList();
        if (beList != null) {
            list.addAll(beList);
        }
        list.add(id);
        beList = ImmutableList.<Long>copyOf(list);
        GlobalStateMgr.getCurrentSystemInfo().getBackend(id).setResourceGroup(this.name);
    }

    public void addComputeNode(Long id) {
        if (GlobalStateMgr.getCurrentSystemInfo().getComputeNode(id) == null) {
            throw new IllegalArgumentException("Compute node id " + id + " does not exist!");
        }
        if (cnList.contains(id)) {
            throw new IllegalArgumentException("Compute node " + id + ", already added!");
        }
        if (cnList.size() + 1 > maxCnNumber) {
            throw new IllegalArgumentException("Compute number exceed limit " + maxCnNumber
                    + ", please alter `cn.number.max` first");
        }
        List<Long> list = Lists.newArrayList();
        if (cnList != null) {
            list.addAll(cnList);
        }
        list.add(id);
        cnList = ImmutableList.<Long>copyOf(list);
        GlobalStateMgr.getCurrentSystemInfo().getComputeNode(id).setResourceGroup(this.name);
    }

    public void dropBackend(Long id) {
        if (beList == null || !beList.contains(id)) {
            throw new IllegalArgumentException("Backend id " + id + " does not exist!");
        }
        if (beList.size() - 1 > beNumber) {
            throw new IllegalArgumentException("Backend number less than limit " + beNumber
                    + ", please alter `be.number` first");
        }
        List<Long> list = Lists.newArrayList();
        if (beList != null) {
            list.addAll(beList);
        }
        list.remove(id);
        beList = ImmutableList.<Long>copyOf(list);
        Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(id);
        be.setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
    }

    public void dropComputeNode(Long id) {
        if (cnList == null || !cnList.contains(id)) {
            throw new IllegalArgumentException("Computer node id " + id + " does not exist!");
        }
        if (cnList.size() - 1 > cnNumber) {
            throw new IllegalArgumentException("Compute node number less than limit " + cnNumber
                    + ", please alter `cn.number` first");
        }
        List<Long> list = Lists.newArrayList();
        if (cnList != null) {
            list.addAll(cnList);
        }
        list.remove(id);
        cnList = ImmutableList.<Long>copyOf(list);
        ComputeNode cn = GlobalStateMgr.getCurrentSystemInfo().getComputeNode(id);
        cn.setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
    }

    public void setBeList(List<Long> beList) {
        this.beList = ImmutableList.<Long>copyOf(beList);
    }

    // Remove resource group tag from ComputeNode/Backend.
    public void destruct() {
        if (!name.equals(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME)) {
            SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
            cnList.forEach(id -> {
                ComputeNode cn = systemInfoService.getComputeNode(id);
                if (systemInfoService.getComputeNode(id) != null) {
                    cn.setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
                }
            });
            beList.forEach(id -> {
                Backend be = systemInfoService.getBackend(id);
                if (systemInfoService.getBackend(id) != null) {
                    be.setResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
                }
            });
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceGroup resourceGroup = (ResourceGroup) o;
        return id == resourceGroup.id && version == resourceGroup.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version);
    }
}
