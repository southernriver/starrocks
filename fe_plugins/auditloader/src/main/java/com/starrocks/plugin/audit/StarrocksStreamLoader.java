package com.starrocks.plugin.audit;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StarrocksStreamLoader {
    private static final Logger LOG = LogManager.getLogger(StarrocksStreamLoader.class);

    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";

    private String hostPort;

    private String db;

    private String tbl;

    private String user;

    private String passwd;

    private String loadUrlStr;

    private String authEncoding;

    private String feIdentity;

    public StarrocksStreamLoader(AuditLoaderPlugin.AuditLoaderConf conf) {
        this.hostPort = conf.frontendHostPort;
        this.db = conf.database;
        this.tbl = conf.table;
        this.user = conf.user;
        this.passwd = conf.password;
        this.loadUrlStr = String.format(loadUrlPattern, new Object[] { this.hostPort, this.db, this.tbl });
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", new Object[] { this.user, this.passwd }).getBytes(StandardCharsets.UTF_8));
        this.feIdentity = conf.feIdentity.replaceAll("\\.", "_");
        LOG.info("loadUrlStr: " + loadUrlStr);
    }

    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " + this.authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        conn.addRequestProperty("max_filter_ratio", "1.0");
        conn.addRequestProperty("columns", "query_id, time, client_ip, user, resource_group, catalog, db, query_table, state, error_code, query_time, scan_bytes, scan_rows, return_rows, cpu_cost_ns, mem_cost_bytes, stmt_id, is_query, data_source, request_type, frontend_ip, stmt, digest, plan_cpu_costs, plan_mem_costs, exception");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    private String toCurl(HttpURLConnection conn) {
        StringBuilder sb = new StringBuilder("curl -v ");
        sb.append("-X ").append(conn.getRequestMethod()).append(" \\\n  ");
        sb.append("-H \"").append("Authorization\":").append("\"Basic " + this.authEncoding).append("\" \\\n  ");
        sb.append("-H \"").append("Expect\":").append("\"100-continue\" \\\n  ");
        sb.append("-H \"").append("Content-Type\":").append("\"text/plain; charset=UTF-8\" \\\n  ");
        sb.append("-H \"").append("max_filter_ratio\":").append("\"1.0\" \\\n  ");
        sb.append("-H \"").append("columns\":").append("\"query_id, time, client_ip, user, resource_group, catalog, db, query_table, state, error_code, query_time, scan_bytes, scan_rows, return_rows, cpu_cost_ns, mem_cost_bytes, stmt_id, is_query, data_source, request_type, frontend_ip, stmt, digest, plan_cpu_costs, plan_mem_costs, exception\" \\\n  ");
        sb.append("\"").append(conn.getURL()).append("\"");
        return sb.toString();
    }

    private String getContent(HttpURLConnection conn) {
        BufferedReader br = null;
        StringBuilder response = new StringBuilder();
        try {
            if (100 <= conn.getResponseCode() && conn.getResponseCode() <= 399) {
                br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            } else {
                br = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
            }
            String line;
            while ((line = br.readLine()) != null)
                response.append(line);
        } catch (IOException e) {
            LOG.warn("get content error,", e);
        }
        return response.toString();
    }

    public LoadResponse loadBatch(StringBuilder sb) {
        Calendar calendar = Calendar.getInstance();
        String label = String.format("audit_%s%02d%02d_%02d%02d%02d_%s", new Object[] { Integer.valueOf(calendar.get(1)), Integer.valueOf(calendar.get(2) + 1), Integer.valueOf(calendar.get(5)),
                Integer.valueOf(calendar.get(11)), Integer.valueOf(calendar.get(12)), Integer.valueOf(calendar.get(13)), this.feIdentity });
        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            feConn = getConnection(this.loadUrlStr, label);
            int status = feConn.getResponseCode();
            if (status != 307)
                throw new Exception("status is not TEMPORARY_REDIRECT 307, status: " + status + ", response: " +
                        getContent(feConn) + ", request is: " + toCurl(feConn));
            String location = feConn.getHeaderField("Location");
            if (location == null)
                throw new Exception("redirect location is null");
            beConn = getConnection(location, label);
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            bos.write(sb.toString().getBytes());
            bos.close();
            status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            String response = getContent(beConn);
            LOG.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}", label,
                    Integer.valueOf(status), respMsg, response);
            return new LoadResponse(status, respMsg, response);
        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if (feConn != null)
                feConn.disconnect();
            if (beConn != null)
                beConn.disconnect();
        }
    }

    public static class LoadResponse {
        public int status;

        public String respMsg;

        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("status: ").append(this.status);
            sb.append(", resp msg: ").append(this.respMsg);
            sb.append(", resp content: ").append(this.respContent);
            return sb.toString();
        }
    }
}
