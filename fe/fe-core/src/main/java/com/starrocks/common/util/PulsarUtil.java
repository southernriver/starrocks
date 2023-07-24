// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PPulsarLoadInfo;
import com.starrocks.proto.PPulsarMetaProxyRequest;
import com.starrocks.proto.PPulsarPositionProxyRequest;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PStringPair;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);

    private static final PulsarUtil.ProxyAPI PROXY_API = new PulsarUtil.ProxyAPI();

    public static List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                      ImmutableMap<String, String> properties) throws UserException {
        return PROXY_API.getAllPulsarPartitions(serviceUrl, topic, subscription, properties);
    }

    public static Map<String, byte[]> getPositions(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> properties, List<String> partitions)
            throws UserException {
        return PROXY_API.getPositions(serviceUrl, topic, subscription, properties, partitions);
    }

    public static PPulsarLoadInfo genPPulsarLoadInfo(String serviceUrl, String topic, String subscription,
                                                     ImmutableMap<String, String> properties) {
        PPulsarLoadInfo pulsarLoadInfo = new PPulsarLoadInfo();
        pulsarLoadInfo.serviceUrl = serviceUrl;
        pulsarLoadInfo.topic = topic;
        pulsarLoadInfo.subscription = subscription;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PStringPair pair = new PStringPair();
            pair.key = entry.getKey();
            pair.val = entry.getValue();
            if (pulsarLoadInfo.properties == null) {
                pulsarLoadInfo.properties = Lists.newArrayList();
            }
            pulsarLoadInfo.properties.add(pair);
        }
        return pulsarLoadInfo;
    }

    static class ProxyAPI {
        public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            PPulsarMetaProxyRequest metaRequest = new PPulsarMetaProxyRequest();
            metaRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, convertedCustomProperties);
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarMetaRequest = metaRequest;

            PPulsarProxyResult result = sendProxyRequest(request);
            return result.pulsarMetaResult.partitions;
        }

        public Map<String, byte[]> getPositions(String serviceUrl, String topic, String subscription,
                                                ImmutableMap<String, String> properties, List<String> partitions)
                throws UserException {
            // create request
            PPulsarPositionProxyRequest positionRequest = new PPulsarPositionProxyRequest();
            positionRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, properties);
            positionRequest.partitions = partitions;
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarPositionRequest = positionRequest;

            // send request
            PPulsarProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<String, byte[]> partitionPositions = Maps.newHashMapWithExpectedSize(partitions.size());
            List<byte[]> positions = result.pulsarPositionResult.messageIds;
            for (int i = 0; i < result.pulsarPositionResult.partitions.size(); i++) {
                partitionPositions.put(result.pulsarPositionResult.partitions.get(i), positions.get(i));
            }
            return partitionPositions;
        }

        private PPulsarProxyResult sendProxyRequest(PPulsarProxyRequest request) throws UserException {
            TNetworkAddress address = new TNetworkAddress();
            try {
                List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
                if (backendIds.isEmpty()) {
                    throw new LoadException("Failed to send proxy request. No alive backends");
                }
                Collections.shuffle(backendIds);
                Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                request.timeout = Config.routine_load_pulsar_timeout_second;
                Future<PPulsarProxyResult> future = BackendServiceClient.getInstance().getPulsarInfo(address, request);
                PPulsarProxyResult result = future.get(Config.routine_load_pulsar_timeout_second, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                if (code != TStatusCode.OK) {
                    LOG.warn("failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                    throw new UserException(
                            "failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                } else {
                    return result;
                }
            } catch (InterruptedException ie) {
                LOG.warn("got interrupted exception when sending proxy request to " + address);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

