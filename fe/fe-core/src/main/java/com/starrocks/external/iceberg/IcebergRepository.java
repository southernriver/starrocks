// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.common.Config;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class IcebergRepository {
    private static final Logger LOG = LogManager.getLogger(IcebergRepository.class);

    public IcebergRepository() {
        if (Config.enable_iceberg_custom_worker_thread) {
            // TODO this should be also changed to runtime session conf.
            int availableProcessors = Runtime.getRuntime().availableProcessors();
            int workerThreadPoolSize = Math.min(availableProcessors, Config.iceberg_worker_num_threads);
            LOG.info("Available processors is {}, default iceberg_worker_num_threads is {}, and worker " +
                    "thread pool size should change to {}.", availableProcessors, Config.iceberg_worker_num_threads,
                    workerThreadPoolSize);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(workerThreadPoolSize));
        }
    }
}
