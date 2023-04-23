// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Logger to log specific extra message to extra.log
 */
public class ExtraInfoLogger {
    private static final Logger LOG = LogManager.getLogger(ExtraInfoLogger.class);

    private ExtraInfoLogger(){
    }

    public static Logger getLogger() {
        return LOG;
    }
}
