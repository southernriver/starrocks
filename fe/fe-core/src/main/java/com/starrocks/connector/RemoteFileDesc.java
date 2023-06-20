// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.TextFileFormatDesc;
import org.apache.iceberg.FileScanTask;

import java.util.ArrayList;
import java.util.List;

public class RemoteFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private ImmutableList<RemoteFileBlockDesc> blockDescs;
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;
    private ImmutableList<String> hudiDeltaLogs;

    // Only this single RemoteFileDesc instance is used to record all iceberg scanTask
    // to reduce the memory usage of RemoteFileInfo
    private List<FileScanTask> icebergScanTasks = new ArrayList<>();

    public RemoteFileDesc(String fileName, String compression, long length,
                          ImmutableList<RemoteFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
    }

    public RemoteFileDesc(List<FileScanTask> tasks) {
        icebergScanTasks = Lists.newArrayList(tasks);
    }

    public String getFileName() {
        return fileName;
    }

    public String getCompression() {
        return compression;
    }

    public long getLength() {
        return length;
    }

    public ImmutableList<RemoteFileBlockDesc> getBlockDescs() {
        return blockDescs;
    }

    public boolean isSplittable() {
        return splittable;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public RemoteFileDesc setSplittable(boolean splittable) {
        this.splittable = splittable;
        return this;
    }

    public RemoteFileDesc setTextFileFormatDesc(TextFileFormatDesc textFileFormatDesc) {
        this.textFileFormatDesc = textFileFormatDesc;
        return this;
    }

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }

    public List<FileScanTask> getIcebergScanTasks() {
        return icebergScanTasks;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoteFileDesc{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", compression='").append(compression).append('\'');
        sb.append(", length=").append(length);
        sb.append(", blockDescs=").append(blockDescs);
        sb.append(", splittable=").append(splittable);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append(", hudiDeltaLogs=").append(hudiDeltaLogs);
        sb.append('}');
        return sb.toString();
    }
}
