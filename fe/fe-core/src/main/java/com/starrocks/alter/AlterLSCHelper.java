// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/AlterLightSchChangeHelper.java

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
package com.starrocks.alter;


import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AlterLightSchemaChangeInfo;
import com.starrocks.proto.PFetchColIdsRequest;
import com.starrocks.proto.PFetchColIdsRequest.PFetchColIdParam;
import com.starrocks.proto.PFetchColIdsResponse;
import com.starrocks.proto.PFetchColIdsResponse.PFetchColIdsResultEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * For alter light_schema_change table property
 */
public class AlterLSCHelper {

    private static final Logger LOG = LogManager.getLogger(AlterLSCHelper.class);

    private final Database db;

    private final OlapTable olapTable;

    public AlterLSCHelper(Database db, OlapTable olapTable) {
        this.db = db;
        this.olapTable = olapTable;
    }

    /**
     * 1. rpc read columnUniqueIds from BE
     * 2. refresh table metadata
     * 3. write edit log
     */
    public void enableLightSchemaChange() throws DdlException {
        final Map<Long, PFetchColIdsRequest> params = initParams();
        final AlterLightSchemaChangeInfo info = callForColumnsInfo(params);
        updateTableMeta(info);
        GlobalStateMgr.getCurrentState().getEditLog().logAlterLightSchemaChange(info);
        LOG.info("successfully enable `light_schema_change`");
    }

    /**
     * This method creates RPC params for several BEs which target tablets lie on.
     * Each param contains several target indexIds and each indexId is mapped to all the tablet ids on it.
     * We should pass all tablet id for a consistency check of index schema.
     *
     * @return beId -> set(indexId -> tabletIds)
     */
    private Map<Long, PFetchColIdsRequest> initParams() {
        // params: indexId -> tabletIds
        Map<Long, Map<Long, Set<Long>>> beIdToRequestInfo = new HashMap<>();
        final Collection<Partition> partitions = olapTable.getAllPartitions();
        for (Partition partition : partitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    buildParams(index.getId(), tablet, beIdToRequestInfo);
                }
            }
        }
        // transfer to rpc params
        final Map<Long, PFetchColIdsRequest> beIdToRequest = new HashMap<>();
        beIdToRequestInfo.keySet().forEach(beId -> {
            final Map<Long, Set<Long>> indexIdToTabletIds = beIdToRequestInfo.get(beId);
            final PFetchColIdsRequest fetchColIdsRequest = new PFetchColIdsRequest();
            fetchColIdsRequest.params = new ArrayList<>();
            for (Long indexId : indexIdToTabletIds.keySet()) {
                final PFetchColIdParam fetchColIdParam = new PFetchColIdParam();
                fetchColIdParam.indexId = indexId;
                fetchColIdParam.tabletIds = new ArrayList<>();
                fetchColIdParam.tabletIds.addAll(indexIdToTabletIds.get(indexId));
                fetchColIdsRequest.params.add(fetchColIdParam);
            }
            beIdToRequest.put(beId, fetchColIdsRequest);
        });
        return beIdToRequest;
    }

    private void buildParams(Long indexId, Tablet tablet, Map<Long, Map<Long, Set<Long>>> beIdToRequestInfo) {
        final Set<Long> backendIds = tablet.getBackendIds();
        for (Long backendId : backendIds) {
            beIdToRequestInfo.putIfAbsent(backendId, new HashMap<>());
            final Map<Long, Set<Long>> indexIdToTabletId = beIdToRequestInfo.get(backendId);
            indexIdToTabletId.putIfAbsent(indexId, new HashSet<>());
            indexIdToTabletId.computeIfPresent(indexId, (idxId, set) -> {
                set.add(tablet.getId());
                return set;
            });
        }
    }

    /**
     * @param beIdToRequest rpc param for corresponding BEs
     * @return contains indexIds to each tablet schema info which consists of columnName to corresponding
     * column unique id pairs
     * @throws DdlException as a wrapper for rpc failures
     */
    private AlterLightSchemaChangeInfo callForColumnsInfo(Map<Long, PFetchColIdsRequest> beIdToRequest)
            throws DdlException {
        final List<Future<PFetchColIdsResponse>> futureList = new ArrayList<>();
        // start a rpc in a pipeline way
        try {
            for (Long beId : beIdToRequest.keySet()) {
                final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend().get(beId);
                final TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
                final Future<PFetchColIdsResponse> responseFuture = BackendServiceClient.getInstance()
                        .getColIdsByTabletIds(address, beIdToRequest.get(beId));
                futureList.add(responseFuture);
            }
        } catch (RpcException e) {
            throw new DdlException("fetch columnIds RPC failed", e);
        }
        // wait for and get results
        final long start = System.currentTimeMillis();
        long timeoutMs = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000L;
        final List<PFetchColIdsResponse> resultList = new ArrayList<>();
        try {
            for (Future<PFetchColIdsResponse> future : futureList) {
                final PFetchColIdsResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
                if (response.status.statusCode != TStatusCode.OK.getValue()) {
                    throw new DdlException(response.status.errorMsgs.get(0));
                }
                resultList.add(response);
                // refresh the timeout
                final long now = System.currentTimeMillis();
                final long deltaMs = now - start;
                timeoutMs -= deltaMs;
                Preconditions.checkState(timeoutMs >= 0,
                        "impossible state, timeout should happened");
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new DdlException("fetch columnIds RPC result failed: ", e);
        } catch (TimeoutException e) {
            throw new DdlException("fetch columnIds RPC result timeout", e);
        }
        return compactToAlterLscInfo(resultList);
    }

    /**
     * Since the result collected from several BEs may contain repeated indexes in distributed storage scenarios,
     * we should do consistency check for the result for the same index, and get the unique result.
     */
    private AlterLightSchemaChangeInfo compactToAlterLscInfo(List<PFetchColIdsResponse> resultList) {
        final PFetchColIdsResponse returnResponse = new PFetchColIdsResponse();
        Map<Long, Map<String, Integer>> indexIdToTabletInfo = new HashMap<>();
        returnResponse.entries = new ArrayList<>();
        resultList.forEach(response -> {
            for (PFetchColIdsResultEntry entry : response.entries) {
                final long indexId = entry.indexId;
                if (!indexIdToTabletInfo.containsKey(indexId)) {
                    indexIdToTabletInfo.put(indexId, entry.colNameToId);
                    returnResponse.entries.add(entry);
                    continue;
                }
                // check tablet schema info consistency
                final Map<String, Integer> colNameToId = indexIdToTabletInfo.get(indexId);
                Preconditions.checkState(colNameToId.equals(entry.colNameToId),
                        "index: " + indexId + "got inconsistent schema in storage");
            }
        });
        return new AlterLightSchemaChangeInfo(db.getId(), olapTable.getId(), indexIdToTabletInfo);
    }

    public void updateTableMeta(AlterLightSchemaChangeInfo info) throws DdlException {
        Preconditions.checkNotNull(info, "passed in info should be not null");

        // update index-meta once and for all
        List<Column> baseColumns = olapTable.getSchemaByIndexId(olapTable.getBaseIndexId());
        Map<String, Integer> colNameToId = info.getIndexIdToColumnInfo().get(olapTable.getBaseIndexId());
        Preconditions.checkState(baseColumns.size() == colNameToId.size(),
                "size mismatch for original baseColumns meta and that in change info");

        int maxColId = Column.COLUMN_UNIQUE_ID_INIT_VALUE;
        final List<Column> newSchema = new ArrayList<>();
        for (Column column : baseColumns) {
            final String columnName = column.getName();
            final int columnId = Preconditions.checkNotNull(colNameToId.get(columnName),
                    "failed to fetch column id of column:{" + columnName + "}");
            final Column newColumn = new Column(column);
            newColumn.setUniqueId(columnId);
            newSchema.add(newColumn);
            maxColId = Math.max(columnId, maxColId);
        }

        // update index-meta once and for base
        olapTable.setMaxColUniqueId(maxColId);
        MaterializedIndexMeta baseIndex = olapTable.getIndexMetaByIndexId(olapTable.getBaseIndexId());
        baseIndex.setSchema(newSchema);

        // write table property
        olapTable.setUseLightSchemaChange(true);
        LOG.info("successfully update table meta for `light_schema_change`");
    }
}
