// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

/**
 * Http请求工具类
 * Created by andrewcheng on 2022/9/16.
 */
public class RestClient {
    private static final Logger LOG = LogManager.getLogger(RestClient.class);
    private final CloseableHttpClient client;
    public RestClient(Integer timeOut) {
        client = getHttpClient(timeOut);
    }

    private static CloseableHttpClient getHttpClient(Integer timeOut) {
        final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(200);
        connManager.setDefaultMaxPerRoute(20);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(timeOut)
                .setConnectTimeout(timeOut)
                .setSocketTimeout(timeOut)
                .build();
        HttpRequestRetryHandler retry = (exception, executionCount, context) -> {
            if (executionCount >= 3) {
                //如果已经重试了3次，就放弃
                return false;
            }
            if (exception instanceof ConnectTimeoutException) {
                //连接被拒绝
                return false;
            }
            if (exception instanceof NoHttpResponseException) {
                //如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {
                //不要重试SSL握手异常
                return false;
            }
            if (exception instanceof InterruptedIOException) {
                //超时
                return true;
            }
            if (exception instanceof UnknownHostException) {
                //目标服务器不可达
                return false;
            }
            if (exception instanceof SSLException) {
                //ssl握手异常
                return false;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest)) {
                return true;
            }
            return false;
        };
        return HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setRetryHandler(retry)
                .setConnectionManager(connManager)
                .build();
    }

    public String httpGet(String url) throws IOException {
        String content = "";
        CloseableHttpResponse response = null;
        HttpGet request = new HttpGet(url);
        try {
            response = client.execute(request);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new IOException("Failed to get http response, due to http code: "
                        + response.getStatusLine().getStatusCode() + ", response: " + content);
            }
            HttpEntity entity = response.getEntity();
            content = EntityUtils.toString(entity, "UTF-8");
        } finally {
            if (null != response) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (Exception ex) {
                    LOG.warn("Error during HTTP connection cleanup", ex);
                }
            }
            request.releaseConnection();
        }
        return content;
    }
}
