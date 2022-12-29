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
package com.starrocks.mysql.security;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.LeaderOpExecutor;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utils.TdwUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created by andrewcheng on 2022/8/8.
 */
public class TdwAuthenticate {
    private static final Logger LOG = LogManager.getLogger(TdwAuthenticate.class);

    public static boolean authenticate(byte[] remotePasswd, byte[] randomString, String user,
                                       List<UserIdentity> currentUserIdentity) {
        // check user password by TDW server.
        try {
            if (!TdwUtil.checkPassword(user, remotePasswd, randomString)) {
                LOG.debug("user:{} use error TDW password", user);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Check Tdw password error.", e);
            return false;
        }

        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
        // Search the user in starrocks.
        if (!GlobalStateMgr.getCurrentState().getAuth().doesUserExist(userIdentity)) {
            LOG.debug("User:{} does not exists in starrocks, create by internal.", user);
            if (!createUser(userIdentity)) {
                LOG.error("Failed to create user internally.");
                return false;
            }
        }
        currentUserIdentity.add(userIdentity);
        return true;
    }

    public static boolean useTdwAuthenticate(String user) {
        // The root and admin cannot use tdw authentication.
        if (user.equals(Auth.ROOT_USER) || user.equals(Auth.ADMIN_USER)) {
            return false;
        }
        // If Tdw authentication is enabled and the user exists in TDW, use TDW authentication,
        // otherwise use default authentication.
        return Config.enable_tdw_authentication && TdwUtil.doesUserExist(TdwUtil.getUserName(user));
    }

    private static boolean createUser(UserIdentity userIdentity) {
        String userName = ClusterNamespace.getNameFromFullName(userIdentity.getQualifiedUser());
        // forward to master if necessary
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            String showProcStmt = "CREATE USER \"" + userName + "\"";
            // ConnectContext build in RestBaseAction
            ConnectContext context = ConnectContext.get();
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setQueryId(UUIDUtil.genUUID());
            context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
            LeaderOpExecutor leaderOpExecutor = new LeaderOpExecutor(new OriginStatement(showProcStmt, 0), context,
                    RedirectStatus.FORWARD_NO_SYNC);
            LOG.debug("need to transfer to Leader. stmt: {}", context.getStmtId());

            try {
                leaderOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("failed to forward stmt", e);
                return false;
            }
        } else {
            try {
                GlobalStateMgr.getCurrentState().getAuth().createUserInternal(
                        userIdentity, null, new Password(new byte[0]), false, false);
            } catch (DdlException e) {
                LOG.error("failed to create user " + userIdentity.getQualifiedUser());
                return false;
            }
        }
        return true;
    }
}