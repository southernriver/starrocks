// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/iceberg/pull/4518

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

package com.starrocks.connector.iceberg.io;

import com.starrocks.connector.iceberg.StarRocksIcebergException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;

/**
 * Implementation of FileIO that using specified ugi.
 */
public class IcebergFileIOWithUgi implements FileIO {

    private final FileIO wrappedIO;
    private final UserGroupInformation ugi;

    public IcebergFileIOWithUgi(FileIO io, UserGroupInformation ugi) {
        this.wrappedIO = io;
        this.ugi = ugi;
    }

    @Override
    public InputFile newInputFile(String path) {
        try {
            return Util.doAsWithUGI(ugi, () -> wrappedIO.newInputFile(path));
        } catch (IOException e) {
            throw new StarRocksIcebergException(e.getMessage(), e);
        }
    }

    @Override
    public OutputFile newOutputFile(String path) {
        try {
            return Util.doAsWithUGI(ugi, () -> wrappedIO.newOutputFile(path));
        } catch (IOException e) {
            throw new StarRocksIcebergException(e.getMessage(), e);
        }
    }

    @Override
    public void deleteFile(String path) {
        try {
            Util.doAsWithUGI(ugi, () -> {
                wrappedIO.deleteFile(path);
                return null;
            });
        } catch (IOException e) {
            throw new StarRocksIcebergException(e.getMessage(), e);
        }
    }
}
