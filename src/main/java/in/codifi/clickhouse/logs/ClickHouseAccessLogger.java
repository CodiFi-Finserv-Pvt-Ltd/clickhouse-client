package in.codifi.clickhouse.logs;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.fallback.FallbackConfig;
import in.codifi.clickhouse.logs.fallback.FallbackManager;
import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;

/**
 * Public entry point for the codifi-clickhouse-logs-core library.
 *
 * <p>Usage:
 * <pre>
 *   ClickHouseAccessLogger logger = new ClickHouseAccessLogger(dataSource, 2000);
 *   logger.add(restLog);
 *   logger.addAccessLog(accessLog);
 *   // on application shutdown:
 *   logger.shutdown();
 * </pre>
 */
public class ClickHouseAccessLogger {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseAccessLogger.class);

    private final AccessLogBatchService batchService;

    // Default values used when shorter constructors delegate.
    private static final int    DEFAULT_BATCH_SIZE             = 2000;
    private static final int    DEFAULT_FLUSH_INTERVAL_SECONDS = 60;
    private static final boolean DEFAULT_CREATE_TABLES         = true;
    private static final String DEFAULT_DATABASE               = "logs_db";
    private static final boolean DEFAULT_MASKING_ENABLED       = false;
    private static final long   DEFAULT_MAX_BATCH_BYTES        = 10_000_000L;
    private static final long   DEFAULT_RETRY_BUFFER_MAX_BYTES = 50_000_000L;
    private static final int    DEFAULT_TABLE_RETRY_SECONDS    = 300;

    // ---- existing constructors — all delegate, kept for source/binary compat ----

    public ClickHouseAccessLogger(DataSource ds, int batchSize) {
        this(ds, batchSize, DEFAULT_FLUSH_INTERVAL_SECONDS, DEFAULT_CREATE_TABLES, DEFAULT_DATABASE,
                DEFAULT_MASKING_ENABLED, DEFAULT_MAX_BATCH_BYTES, DEFAULT_RETRY_BUFFER_MAX_BYTES,
                DEFAULT_TABLE_RETRY_SECONDS);
    }

    public ClickHouseAccessLogger(DataSource ds, int batchSize, boolean createTables, String database) {
        this(ds, batchSize, DEFAULT_FLUSH_INTERVAL_SECONDS, createTables, database,
                DEFAULT_MASKING_ENABLED, DEFAULT_MAX_BATCH_BYTES, DEFAULT_RETRY_BUFFER_MAX_BYTES,
                DEFAULT_TABLE_RETRY_SECONDS);
    }

    public ClickHouseAccessLogger(DataSource ds, int batchSize, int flushIntervalSeconds,
            boolean createTables, String database) {
        this(ds, batchSize, flushIntervalSeconds, createTables, database,
                DEFAULT_MASKING_ENABLED, DEFAULT_MAX_BATCH_BYTES, DEFAULT_RETRY_BUFFER_MAX_BYTES,
                DEFAULT_TABLE_RETRY_SECONDS);
    }

    public ClickHouseAccessLogger(DataSource ds, int batchSize, int flushIntervalSeconds,
            boolean createTables, String database, boolean maskingEnabled) {
        this(ds, batchSize, flushIntervalSeconds, createTables, database, maskingEnabled,
                DEFAULT_MAX_BATCH_BYTES, DEFAULT_RETRY_BUFFER_MAX_BYTES, DEFAULT_TABLE_RETRY_SECONDS);
    }

    // ---- 2.0.0 full constructor ----

    /**
     * @param ds                    DataSource pointing at ClickHouse.
     * @param batchSize             Entries to accumulate before a count-based flush.
     * @param flushIntervalSeconds  Periodic flush interval; values below 1 default to 60.
     * @param createTables          When true, creates/verifies the two fixed tables on startup.
     * @param database              ClickHouse database name (e.g. "logs_db").
     * @param maskingEnabled        When true, req/res bodies are masked before insert.
     * @param maxBatchBytes         Byte threshold that triggers an immediate flush (default 10 MB).
     * @param retryBufferMaxBytes   Max bytes kept in the in-memory retry buffer (default 50 MB).
     *                              Oldest batches are evicted when the cap is exceeded.
     * @param tableRetrySeconds     Seconds between table-creation retries if ClickHouse is down (default 300).
     */
    public ClickHouseAccessLogger(DataSource ds, int batchSize, int flushIntervalSeconds,
            boolean createTables, String database, boolean maskingEnabled,
            long maxBatchBytes, long retryBufferMaxBytes, int tableRetrySeconds) {
        this(ds, batchSize, flushIntervalSeconds, createTables, database, maskingEnabled,
                maxBatchBytes, retryBufferMaxBytes, tableRetrySeconds, FallbackConfig.disabled());
    }

    // ---- 2.2.0 constructor with disk/S3 fallback ----

    /**
     * Same as the 2.0.0 constructor, plus a fallback store for failed batches.
     *
     * @param fallbackConfig  Disk/S3 fallback settings (see {@link FallbackConfig#fromProperties}).
     *                        {@code null}, disabled, or invalid config logs a WARN where relevant and
     *                        keeps the in-memory retry buffer — it never fails construction.
     */
    public ClickHouseAccessLogger(DataSource ds, int batchSize, int flushIntervalSeconds,
            boolean createTables, String database, boolean maskingEnabled,
            long maxBatchBytes, long retryBufferMaxBytes, int tableRetrySeconds,
            FallbackConfig fallbackConfig) {

        int interval = flushIntervalSeconds < 1 ? 60 : flushIntervalSeconds;
        if (interval != flushIntervalSeconds) {
            log.warn("flushIntervalSeconds ({}) is invalid; defaulting to 60", flushIntervalSeconds);
        }

        AccessLogWriter writer = new AccessLogWriter(ds, maskingEnabled);
        AccessLogCache  cache  = new AccessLogCache();
        FallbackManager fallback = FallbackManager.create(fallbackConfig, ds, database, maskingEnabled);
        this.batchService      = new AccessLogBatchService(cache, writer, batchSize, interval,
                database, maxBatchBytes, retryBufferMaxBytes, fallback);

        if (createTables) {
            AccessLogTableManager tableManager = new AccessLogTableManager(ds, database);
            Thread initThread = new Thread(() -> {
                while (true) {
                    try {
                        if (tableManager.ensureTables()) {
                            batchService.scheduleDaily(() -> tableManager.ensureTables());
                            log.info("ClickHouse tables ready in database '{}'", database);
                            return;
                        }
                    } catch (Exception e) {
                        log.error("ensureTables() threw unexpectedly: {}", e.getMessage(), e);
                    }
                    log.warn("Tables not ready; retrying in {} s", tableRetrySeconds);
                    try {
                        Thread.sleep(tableRetrySeconds * 1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
            initThread.setDaemon(true);
            initThread.setName("ch-table-init");
            initThread.start();
        }
    }

    // ---- public API ----

    /** Enqueues a REST API access log entry. */
    public void add(RestAccessLogModel model) {
        batchService.add(model);
    }

    /** Enqueues a general access log entry. */
    public void addAccessLog(AccessLogModel model) {
        batchService.addAccessLog(model);
    }

    /** Enqueues a user login-report entry. */
    public void addLoginReport(UserLoginReportModel model) {
        batchService.addLoginReport(model);
    }

    /** Flushes remaining entries and shuts down thread pools. Call once on shutdown. */
    public void shutdown() {
        batchService.shutdown();
    }
}
