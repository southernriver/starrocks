// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwRestClient extends RestClient {
    private static final Logger LOG = LogManager.getLogger(TdwRestClient.class);


    private ExecutorService executor =
            ThreadPoolManager.newDaemonCacheThreadPool(256, 256, "get-tdw-user-cache", false);
    private LoadingCache<String, String> userCache;
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
        userCache  = newCacheBuilder(Config.tdw_user_cache_count, Config.tdw_user_cache_ttl_s)
                .build(asyncReloading(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return loadPassword(key);
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

    private TdwResponse getTdwResponse(String url) throws IOException {
        String content = httpGet(url);
        DsfResponse dsfResponse = JsonUtil.readValue(content, DsfResponse.class);
        if (dsfResponse.getRetCode() != 0) {
            throw new IOException("Failed to get tdw response, due to retCode: "
                    + dsfResponse.getRetCode() + ", retMsg: " + dsfResponse.getRetMsg());
        }
        return dsfResponse.getRetObj();
    }
}
