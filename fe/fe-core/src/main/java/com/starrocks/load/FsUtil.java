// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/ExportJob.java

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

package com.starrocks.load;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.UserException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.thrift.TBrokerFileStatus;

import java.util.List;

public class FsUtil {

    public static boolean checkPathExist(String path, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            return BrokerUtil.checkPathExist(path, brokerDesc);
        } else {
            return HdfsUtil.checkPathExist(path, brokerDesc);
        }
    }

    public static void deletePath(String path, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.deletePath(path, brokerDesc);
        } else {
            HdfsUtil.deletePath(path, brokerDesc);
        }
    }

    public static void rename(String from, String to, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.rename(from, to, brokerDesc);
        } else {
            HdfsUtil.rename(from, to, brokerDesc);
        }
    }

    public static void rename(String from, String to, BrokerDesc brokerDesc, int timeoutMs) throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.rename(from, to, brokerDesc, timeoutMs);
        } else {
            HdfsUtil.rename(from, to, brokerDesc, timeoutMs);
        }
    }

    public static void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
        } else {
            HdfsUtil.parseFile(path, brokerDesc, fileStatuses);
        }
    }

    public static byte[] readFile(String dppResultFilePath, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            return BrokerUtil.readFile(dppResultFilePath, brokerDesc);
        } else {
            return HdfsUtil.readFile(dppResultFilePath, brokerDesc);
        }
    }

    public static void writeFile(byte[] configData, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.writeFile(configData, destFilePath, brokerDesc);
        } else {
            HdfsUtil.writeFile(configData, destFilePath, brokerDesc);
        }
    }

    public static void writeFile(String srcFilePath, String destFilePath, BrokerDesc brokerDesc) throws UserException {
        if (brokerDesc.hasBroker()) {
            BrokerUtil.writeFile(srcFilePath, destFilePath, brokerDesc);
        } else {
            HdfsUtil.writeFile(srcFilePath, destFilePath, brokerDesc);
        }
    }
}
