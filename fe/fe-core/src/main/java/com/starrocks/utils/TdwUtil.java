// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.ConnectContext;
import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.ServiceTarget;
import com.tencent.tdw.security.authentication.client.SecureClient;
import com.tencent.tdw.security.authentication.client.SecureClientFactory;
import com.tencent.tdw.security.exceptions.SecureException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwUtil {
    public static final String TDW_PARTITION_PREFIX_SEPARATOR = ";";
    public static final String TDW_PARTITION_DEFAULT_PREFIX = "p_";
    private static final String SUPERSQL_DRIVER = "com.tencent.supersql.jdbc.SuperSqlDriver";
    private static final ThreadLocal<String> TDW_USER_THREADLOCAL_INFO = new ThreadLocal<>();

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
        return username;
    }

    public static String getUserName(String username) {
        if (StringUtils.isNotEmpty(username)) {
            return username.startsWith("tdw_") ?
                    username.substring(4) : username;
        }
        return username;
    }

    public static String getCurrentTdwUserName() {
        if (TDW_USER_THREADLOCAL_INFO.get() != null) {
            return TDW_USER_THREADLOCAL_INFO.get();
        }
        if (ConnectContext.get() != null) {
            return getTdwUserName(ConnectContext.get().getQualifiedUser());
        }
        return null;
    }

    public static void setCurrentTdwUserName(String tdwUserName) {
        TDW_USER_THREADLOCAL_INFO.set(tdwUserName);
    }

    public static void removeCurrentTdwUserName() {
        TDW_USER_THREADLOCAL_INFO.remove();
    }

    public static boolean hasQueryPrivilege(String dbName, String tableName) throws AnalysisException {
        String userName = getCurrentTdwUserName();
        if (userName == null) {
            userName = "root";
        }
        return TdwRestClient.getInstance().queryPrivilegeForTable(userName, dbName, tableName);
    }

    private static String generateAuth(String userName) throws SecureException {
        SecureClient secureClient = SecureClientFactory.generate(Config.supersql_proxy_platform,
                LocalKeyManager.generateByDefaultKey(Config.supersql_proxy_platform_key));
        String service = "supersql";
        // username is user's rtx
        Authentication authentication = secureClient.getAuthentication(ServiceTarget.valueOf(service), userName);
        return authentication.flat();
    }

    public static void addPartition(String dbName, String tableName, String partition, String userName) {
        String partitionValue;
        int hasPartitionPrefix = partition.indexOf(TDW_PARTITION_PREFIX_SEPARATOR);
        if (hasPartitionPrefix > 0) {
            partitionValue = partition.substring(hasPartitionPrefix + 1);
            partition = partition.replaceAll(TDW_PARTITION_PREFIX_SEPARATOR, "");
        } else {
            partitionValue = partition;
            partition = TDW_PARTITION_DEFAULT_PREFIX + partition;
        }
        partition = partition.replaceAll("-", "");
        String sql =
                String.format("ALTER TABLE %s ADD PARTITION `%s` VALUES IN ('%s')", tableName, partition, partitionValue);
        executeInSuperSql(userName, dbName, sql);
    }

    private static void executeInSuperSql(String userName, String dbName, String sql) {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            String url = "jdbc:supersql:url=http://" + Config.supersql_public_url;

            Class.forName(SUPERSQL_DRIVER);

            Properties p = new Properties();
            // tAuth
            p.put("rawAuth", generateAuth(userName));

            Driver driver = DriverManager.getDriver(url);
            conn = driver.connect(url, p);

            st = conn.createStatement();

            st.execute("set `tdw.ugi.apptype` = jdbc");
            st.execute("use " + dbName);

            st.execute(sql);
        } catch (Exception e) {
            if (e.getMessage().contains("have already contain")) {
                return;
            }
            throw new StarRocksConnectorException("failed to execute sql: " + sql + ", " + e.getMessage(), e);
        } finally {
            try {
                rs.close();
            } catch (Exception ee) {
                // handle exception
            }

            try {
                // st.close() is synchronous and blocked until current query execution is completed
                // use forceClose() instead for immediate query cancellation and resource cleaning
                ((AvaticaStatement) st).forceClose();
            } catch (Exception ee) {
                // handle exception
            }

            try {
                conn.close();
            } catch (Exception ee) {
                // handle exception
            }
        }
    }
}
