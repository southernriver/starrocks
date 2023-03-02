// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TIcebergRLTaskProgressSplit;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IcebergSplit implements Writable {
    private final IcebergSplitMeta splitMeta;
    private final String path;
    private final long offset;
    private final long length;
    private final long fileSize;
    // formatType is no need to persist
    private TFileFormatType formatType;

    public IcebergSplit(TIcebergRLTaskProgressSplit tIcebergRLTaskProgressSplit) {
        this.splitMeta = new IcebergSplitMeta(tIcebergRLTaskProgressSplit.start_snapshot_id,
                tIcebergRLTaskProgressSplit.end_snapshot_id, tIcebergRLTaskProgressSplit.end_snapshot_timestamp,
                tIcebergRLTaskProgressSplit.total_splits);
        this.path = tIcebergRLTaskProgressSplit.path;
        this.offset = tIcebergRLTaskProgressSplit.offset;
        this.length = tIcebergRLTaskProgressSplit.length;
        this.fileSize = tIcebergRLTaskProgressSplit.fileSize;
    }

    public IcebergSplit(IcebergSplitMeta splitMeta, ContentScanTask<?> contentScanTask)
            throws RoutineLoadPauseException {
        this.splitMeta = splitMeta;
        ContentFile<?> file = contentScanTask.file();
        this.path = file.path().toString();
        this.offset = contentScanTask.start();
        this.length = contentScanTask.length();
        this.fileSize = file.fileSizeInBytes();
        switch (file.format()) {
            case PARQUET:
                formatType = TFileFormatType.FORMAT_PARQUET;
                break;
            case ORC:
                formatType = TFileFormatType.FORMAT_ORC;
                break;
            default:
                throw new RoutineLoadPauseException(
                        String.format("Cannot read %s file: %s", file.format(), file.path()));

        }
    }

    private IcebergSplit(IcebergSplitMeta splitMeta, String path, long offset, long length, long fileSize) {
        this.splitMeta = splitMeta;
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.fileSize = fileSize;
    }

    public IcebergSplitMeta getSplitMeta() {
        return splitMeta;
    }

    public String getPath() {
        return path;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long getFileSize() {
        return fileSize;
    }

    public TFileFormatType getFormatType() {
        return formatType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        splitMeta.write(out);
        out.writeLong(offset);
        out.writeLong(length);
        out.writeLong(fileSize);
        Text.writeString(out, path);
    }

    public static IcebergSplit fromDataInput(DataInput in) throws IOException {
        IcebergSplitMeta splitMeta = IcebergSplitMeta.fromDataInput(in);
        long offset = in.readLong();
        long length = in.readLong();
        long fileSize = in.readLong();
        String path = Text.readString(in);
        return new IcebergSplit(splitMeta, path, offset, length, fileSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergSplit split = (IcebergSplit) o;
        return offset == split.offset && length == split.length
                && fileSize == split.fileSize && path.equals(split.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, offset, length, fileSize);
    }

    @Override
    public String toString() {
        return "IcebergSplit{" +
                "splitMeta=" + splitMeta +
                ", path='" + path + '\'' +
                ", offset=" + offset +
                ", length=" + length +
                ", fileSize=" + fileSize +
                '}';
    }
}
