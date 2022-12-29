// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.tencent.tdw.auth.RangerDorisAuthorizer;
import com.tencent.tdw.auth.doris.DorisPrivilege;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwRestClient extends RestClient {
    private static final Logger LOG = LogManager.getLogger(TdwRestClient.class);


    private final ExecutorService executor =
            ThreadPoolManager.newDaemonCacheThreadPool(256, 256, "get-user-group-cache", false);
    private LoadingCache<String, String> userCache;
    private LoadingCache<String, List<String>> userGroupCache;

    private static RangerDorisAuthorizer authorizer;
    private static TdwRestClient instance;
    public static TdwRestClient getInstance() {
        if (instance == null) {
            instance = new TdwRestClient();
        }
        return instance;
    }

    public TdwRestClient() {
        super(Config.tdw_check_priv_timeout);
        init();
    }
    private void init() {
        authorizer = new RangerDorisAuthorizer();
        authorizer.init();
        userCache  = newCacheBuilder(Config.tdw_user_cache_count, Config.tdw_user_cache_ttl_s)
                .build(asyncReloading(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return loadPassword(key);
                    }
                }, executor));

        userGroupCache  = newCacheBuilder(Config.tdw_user_cache_count, Config.tdw_user_cache_ttl_s)
                .build(asyncReloading(new CacheLoader<String, List<String>>() {
                    @Override
                    public List<String> load(String key) throws Exception {
                        return loadUserGroup(key);
                    }
                }, executor));
    }
    private CacheBuilder<Object, Object> newCacheBuilder(long maximumSize, long expireTime) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(expireTime, SECONDS);
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    public String getPassword(String userName) {
        try {
            return userCache.get(userName);
        } catch (Exception e) {
            LOG.error("Failed to get user password {}", userName, e);
            return null;
        }
    }

    private String loadPassword(String userName) throws IOException, URISyntaxException {
        JSONArray params = new JSONArray();
        params.put(userName);
        params.put("xxx");
        URIBuilder builder = new URIBuilder();
        builder.setPath(Config.tdw_privilege_api_url + "/AppGroupInternalService")
                .setParameter("m", "mulClusterAppUserPasswdGet")
                .setParameter("p", params.toString());
        TdwResponse response = getTdwResponse(builder.build().toString());

        if (response.getCode() == 100002) {
            LOG.warn("User " + userName + " not found in tdw, return default password.");
            return "";
        }

        if (response.getCode() != 0) {
            throw new IOException("Failed to get user password, due to code: "
                    + response.getCode() + ", message: " + response.getMessage()
                    + ", solution: " + response.getSolution());
        }
        return response.getMessage();
    }

    public List<String> getUserGroup(String userName) {
        try {
            return userGroupCache.get(userName);
        } catch (Exception e) {
            if (e instanceof ExecutionException && e.getCause() instanceof SocketTimeoutException) {
                LOG.error("Get user group of {} timeout, please increase `tdw_check_priv_timeout`", userName, e);
            } else {
                LOG.error("Failed to get user group of {}", userName, e);
            }

            return null;
        }
    }

    private List<String> loadUserGroup(String userName) throws IOException, URISyntaxException {
        JSONArray params = new JSONArray();
        params.put(userName);
        params.put((Object) null);
        URIBuilder builder = new URIBuilder();
        builder.setPath(Config.tdw_privilege_api_url + "/AppGroupInternalService")
                .setParameter("m", "queryAppGroupInfoByUserInfo")
                .setParameter("p", params.toString());
        TdwResponse response = getTdwResponse(builder.build().toString());

        if (response.getCode() != 0) {
            throw new IOException("Failed to get user password, due to code: "
                    + response.getCode() + ", message: " + response.getMessage()
                    + ", solution: " + response.getSolution());
        }
        return response.getAppGroups().stream().map(TdwAppGroup::getAppGroupName).collect(Collectors.toList());
    }

    private TdwResponse getTdwResponse(String url) throws IOException {
        String content = httpGet(url);
        DsfResponse dsfResponse = JsonUtil.readValue(content, DsfResponse.class);
        if (dsfResponse.getRetCode() != 0) {
            throw new IOException("Failed to get tdw response, due to retCode: "
                    + dsfResponse.getRetCode() + ", retMsg: " + dsfResponse.getRetMsg());
        }
        return dsfResponse.getRetObj();
    }

    public boolean queryPrivilegeForTable(String userName, String dbName, String tableName) throws AnalysisException {
        DorisPrivilege privilege = new DorisPrivilege(dbName, tableName, Lists.newArrayList("select"));
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting(userName,
                getUserGroup(userName).toArray(new String[0]));
        try {
            ugi.doAs(
                    (PrivilegedExceptionAction<Boolean>) () -> {
                        authorizer.checkPrivileges(Lists.newArrayList(privilege));
                        return true;
                    }
            );
        } catch (Throwable e) {
            LOG.warn("{} has no select priv on {}.{}", userName, dbName, tableName, e);
            if (e instanceof UndeclaredThrowableException) {
                throw new AnalysisException(((UndeclaredThrowableException) e).getUndeclaredThrowable().getMessage());
            }
        }
        return true;
    }
}