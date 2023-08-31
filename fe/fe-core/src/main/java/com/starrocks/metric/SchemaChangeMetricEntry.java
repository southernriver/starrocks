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

package com.starrocks.metric;

import java.util.Objects;

public class SchemaChangeMetricEntry {
    private long lastFinishTimestamp;
    private double lscMean;
    private double lscMax;
    private double scMean;
    private double scMax;

    public static SchemaChangeMetricEntry create(long lastFinishTimestamp) {
        return new SchemaChangeMetricEntry(lastFinishTimestamp);
    }

    public SchemaChangeMetricEntry(long lastFinishTimestamp) {
        this.lastFinishTimestamp = lastFinishTimestamp;
    }

    public SchemaChangeMetricEntry(long lastFinishTimestamp, double lscMean, double lscMax, double scMean, double scMax) {
        this.lastFinishTimestamp = lastFinishTimestamp;
        this.lscMean = lscMean;
        this.lscMax = lscMax;
        this.scMean = scMean;
        this.scMax = scMax;
    }

    public long getLastFinishTimestamp() {
        return lastFinishTimestamp;
    }

    public SchemaChangeMetricEntry setLastFinishTimestamp(long lastFinishTimestamp) {
        this.lastFinishTimestamp = lastFinishTimestamp;
        return this;
    }

    public double getLscMean() {
        return lscMean;
    }

    public SchemaChangeMetricEntry setLscMean(double lscMean) {
        this.lscMean = lscMean;
        return this;
    }

    public SchemaChangeMetricEntry setLscMax(double lscMax) {
        this.lscMax = lscMax;
        return this;
    }

    public SchemaChangeMetricEntry setScMean(double scMean) {
        this.scMean = scMean;
        return this;
    }

    public SchemaChangeMetricEntry setScMax(double scMax) {
        this.scMax = scMax;
        return this;
    }

    public double getLscMax() {
        return lscMax;
    }

    public double getScMean() {
        return scMean;
    }

    public double getScMax() {
        return scMax;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaChangeMetricEntry that = (SchemaChangeMetricEntry) o;
        return lastFinishTimestamp == that.lastFinishTimestamp && Double.compare(that.lscMean, lscMean) == 0
                && Double.compare(that.lscMax, lscMax) == 0 && Double.compare(that.scMean, scMean) == 0
                && Double.compare(that.scMax, scMax) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastFinishTimestamp, lscMean, lscMax, scMean, scMax);
    }
}
