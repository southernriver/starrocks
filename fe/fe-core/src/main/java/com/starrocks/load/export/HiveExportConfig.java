// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.export;

import java.util.Map;

public class HiveExportConfig {
    private final Map<String, String> targetProperties;

    public HiveExportConfig(Map<String, String> targetProperties) {
        this.targetProperties = targetProperties;
    }

    public void analyzeProperties() {

    }

    public String getPath() {
        return null;
    }
}
