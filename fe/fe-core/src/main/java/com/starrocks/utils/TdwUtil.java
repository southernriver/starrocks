// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

import com.starrocks.mysql.MysqlPassword;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwUtil {

    public static boolean doesUserExist(String userName) {
        return !Arrays.equals(getPassword(userName), MysqlPassword.EMPTY_PASSWORD);
    }

    public static boolean checkPassword(String userName, byte[] remotePasswd, byte[] randomString) {
        // check password
        byte[] saltPassword = MysqlPassword.getSaltFromPassword(getPassword(userName));
        if (saltPassword.length != remotePasswd.length) {
            return false;
        }

        if (remotePasswd.length == 0) {
            return true;
        }
        return MysqlPassword.checkScramble(remotePasswd, randomString, saltPassword);
    }

    private static byte[] getPassword(String userName) {
        userName = getTdwUserName(userName);
        return MysqlPassword.makeScrambledPassword(TdwRestClient.getInstance().getPassword(userName));
    }

    public static String getTdwUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username : String.format(Locale.ROOT, "tdw_%s", username);
        }
        return null;
    }

    public static String getUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username.substring(4) : username;
        }
        return null;
    }
}
