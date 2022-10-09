package com.starrocks.plugin.audit;

import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditPlugin;
import com.starrocks.plugin.Plugin;
import com.starrocks.plugin.PluginContext;
import com.starrocks.plugin.PluginException;
import com.starrocks.plugin.PluginInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AuditLoaderPlugin extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoaderPlugin.class);

    private static SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private StringBuilder auditBuffer = new StringBuilder();

    private long lastLoadTime = 0L;

    private BlockingQueue<AuditEvent> auditEventQueue = new LinkedBlockingDeque<>(1);

    private StarrocksStreamLoader streamLoader;

    private Thread loadThread;

    private AuditLoaderConf conf;

    private volatile boolean isClosed = false;

    private volatile boolean isInit = false;

    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);
        synchronized (this) {
            if (this.isInit)
                return;
            this.lastLoadTime = System.currentTimeMillis();
            loadConfig(ctx, info.getProperties());
            this.streamLoader = new StarrocksStreamLoader(this.conf);
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "audit loader thread");
            this.loadThread.start();
            this.isInit = true;
        }
    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath(), new String[0]);
        if (!Files.exists(pluginPath, new java.nio.file.LinkOption[0]))
            throw new PluginException("plugin path does not exist: " + pluginPath);
        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile, new java.nio.file.LinkOption[0]))
            throw new PluginException("plugin conf file does not exist: " + confFile);
        Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile, new java.nio.file.OpenOption[0])) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }
        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet())
            props.setProperty(entry.getKey(), entry.getValue());
        Map<String, String> properties = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        this.conf = new AuditLoaderConf();
        this.conf.init(properties);
        this.conf.feIdentity = ctx.getFeIdentity();
    }

    public void close() throws IOException {
        super.close();
        this.isClosed = true;
        if (this.loadThread != null)
            try {
                this.loadThread.join();
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return (type == AuditEvent.EventType.AFTER_QUERY);
    }

    public void exec(AuditEvent event) {
        try {
            this.auditEventQueue.add(event);
        } catch (Exception e) {
            LOG.debug("encounter exception when putting current audit batch, discard current audit event", e);
        }
    }

    private void assembleAudit(AuditEvent event) {
        this.auditBuffer.append(event.queryId).append("\t");
        this.auditBuffer.append(longToTimeString(event.timestamp)).append("\t");
        this.auditBuffer.append(event.clientIp).append("\t");
        this.auditBuffer.append(event.user).append("\t");
        this.auditBuffer.append(event.resourceGroup).append("\t");
        this.auditBuffer.append(event.catalog).append("\t");
        this.auditBuffer.append(event.db).append("\t");
        this.auditBuffer.append(event.state).append("\t");
        this.auditBuffer.append(event.errorCode).append("\t");
        this.auditBuffer.append(event.queryTime).append("\t");
        this.auditBuffer.append(event.scanBytes).append("\t");
        this.auditBuffer.append(event.scanRows).append("\t");
        this.auditBuffer.append(event.returnRows).append("\t");
        this.auditBuffer.append(event.cpuCostNs).append("\t");
        this.auditBuffer.append(event.memCostBytes).append("\t");
        this.auditBuffer.append(event.stmtId).append("\t");
        this.auditBuffer.append(event.isQuery ? 1 : 0).append("\t");
        this.auditBuffer.append(event.feIp).append("\t");
        String stmt = truncateByBytes(event.stmt).replace("\t", " ").replace("\n", " ");
        LOG.debug("receive audit event with stmt: {}", stmt);
        this.auditBuffer.append(stmt).append("\t");
        this.auditBuffer.append(event.digest).append("\t");
        this.auditBuffer.append(event.planCpuCosts).append("\t");
        this.auditBuffer.append(event.planMemCosts).append("\n");
    }

    private String truncateByBytes(String str) {
        int maxLen = Math.min(this.conf.max_stmt_length, (str.getBytes()).length);
        if (maxLen >= (str.getBytes()).length)
            return str;
        Charset utf8Charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position());
    }

    private void loadIfNecessary(StarrocksStreamLoader loader) {
        if (this.auditBuffer.length() < this.conf.maxBatchSize && System.currentTimeMillis() - this.lastLoadTime < this.conf.maxBatchIntervalSec * 1000L)
            return;
        this.lastLoadTime = System.currentTimeMillis();
        try {
            StarrocksStreamLoader.LoadResponse response = loader.loadBatch(this.auditBuffer);
            LOG.debug("audit loader response: {}", response);
        } catch (Exception e) {
            LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
        } finally {
            this.auditBuffer = new StringBuilder();
        }
    }

    public static class AuditLoaderConf {
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";

        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";

        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";

        public static final String PROP_USER = "user";

        public static final String PROP_PASSWORD = "password";

        public static final String PROP_DATABASE = "database";

        public static final String PROP_TABLE = "table";

        public static final String MAX_STMT_LENGTH = "max_stmt_length";

        public long maxBatchSize = 52428800L;

        public long maxBatchIntervalSec = 60L;

        public String frontendHostPort = "127.0.0.1:8080";

        public String user = "root";

        public String password = "";

        public String database = "starrocks_audit_db";

        public String table = "starrocks_audit_tbl";

        public String feIdentity = "";

        public int max_stmt_length = 4096;

        public void init(Map<String, String> properties) throws PluginException {
            try {
                if (properties.containsKey("max_batch_size"))
                    this.maxBatchSize = Long.valueOf(properties.get("max_batch_size")).longValue();
                if (properties.containsKey("max_batch_interval_sec"))
                    this.maxBatchIntervalSec = Long.valueOf(properties.get("max_batch_interval_sec")).longValue();
                if (properties.containsKey("frontend_host_port"))
                    this.frontendHostPort = properties.get("frontend_host_port");
                if (properties.containsKey("user"))
                    this.user = properties.get("user");
                if (properties.containsKey("password"))
                    this.password = properties.get("password");
                if (properties.containsKey("database"))
                    this.database = properties.get("database");
                if (properties.containsKey("table"))
                    this.table = properties.get("table");
                if (properties.containsKey("max_stmt_length"))
                    this.max_stmt_length = Integer.parseInt(properties.get("max_stmt_length"));
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        private StarrocksStreamLoader loader;

        public LoadWorker(StarrocksStreamLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!AuditLoaderPlugin.this.isClosed) {
                try {
                    AuditEvent event = AuditLoaderPlugin.this.auditEventQueue.poll(5L, TimeUnit.SECONDS);
                    if (event != null) {
                        AuditLoaderPlugin.this.assembleAudit(event);
                        AuditLoaderPlugin.this.loadIfNecessary(this.loader);
                    }
                } catch (InterruptedException ie) {
                    AuditLoaderPlugin.LOG.debug("encounter exception when loading current audit batch", ie);
                } catch (Exception e) {
                    AuditLoaderPlugin.LOG.error("run audit logger error:", e);
                }
            }
        }
    }

    public static synchronized String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L)
            return "1900-01-01 00:00:00";
        return DATETIME_FORMAT.format(new Date(timeStamp));
    }
}
