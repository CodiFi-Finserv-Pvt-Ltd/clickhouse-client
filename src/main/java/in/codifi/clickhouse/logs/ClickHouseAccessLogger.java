package in.codifi.clickhouse.logs;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.util.CommonUtils;

/**
 * Public entry point for the codifi-clickhouse-logs-core library.
 *
 * <p>Usage:
 * <pre>
 *   ClickHouseAccessLogger logger = new ClickHouseAccessLogger(dataSource, 5);
 *   logger.add(restLog);
 *   logger.addAccessLog(accessLog);
 *   // on application shutdown:
 *   logger.shutdown();
 * </pre>
 *
 * <p>The caller must supply:
 * <ul>
 *   <li>A configured {@link javax.sql.DataSource} pointing at ClickHouse.</li>
 *   <li>A batch size (records to accumulate before flushing to DB).</li>
 *   <li>An SLF4J binding on the classpath (logback, slf4j-simple, etc.).</li>
 * </ul>
 */
public class ClickHouseAccessLogger {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseAccessLogger.class);

    private final AccessLogBatchService batchService;

    /**
     * Creates a logger that automatically ensures ClickHouse tables exist for
     * today and the next 7 days, using database {@code "logs_db"} and a
     * 60-second flush interval.
     *
     * @param ds        A {@link javax.sql.DataSource} backed by the ClickHouse JDBC driver.
     * @param batchSize Number of log entries to accumulate before a batch insert is triggered.
     */
    public ClickHouseAccessLogger(DataSource ds, int batchSize) {
        this(ds, batchSize, 60, true, "logs_db");
    }

    /**
     * @param ds            A {@link javax.sql.DataSource} backed by the ClickHouse JDBC driver.
     * @param batchSize     Number of log entries to accumulate before a batch insert is triggered.
     * @param createTables  When {@code true}, tables are created on startup and refreshed daily.
     *                      SQL errors during table creation are logged, never thrown.
     * @param database      ClickHouse database name in which tables are created.
     */
    public ClickHouseAccessLogger(DataSource ds, int batchSize, boolean createTables, String database) {
        this(ds, batchSize, 60, createTables, database);
    }

    /**
     * @param ds                   A {@link javax.sql.DataSource} backed by the ClickHouse JDBC driver.
     * @param batchSize            Number of log entries to accumulate before a batch insert is triggered.
     * @param flushIntervalSeconds Seconds between scheduled flush attempts; values below 1 are clamped to 60.
     * @param createTables         When {@code true}, tables are created on startup and refreshed daily.
     *                             SQL errors during table creation are logged, never thrown.
     * @param database             ClickHouse database name in which tables are created.
     */
    public ClickHouseAccessLogger(DataSource ds, int batchSize, int flushIntervalSeconds,
            boolean createTables, String database) {
        int interval = flushIntervalSeconds;
        if (interval < 1) {
            log.warn("flushIntervalSeconds ({}) is invalid; defaulting to 60", flushIntervalSeconds);
            interval = 60;
        }

        CommonUtils        commonUtils  = new CommonUtils();
        AccessLogWriter    writer       = new AccessLogWriter(ds, commonUtils);
        AccessLogCache     cache        = new AccessLogCache();
        this.batchService              = new AccessLogBatchService(cache, writer, batchSize, interval);

        if (createTables) {
            AccessLogTableManager tableManager = new AccessLogTableManager(ds, database);
            tableManager.ensureTables();
            batchService.scheduleDaily(tableManager::ensureTables);
        }
    }

    /**
     * Enqueues a REST API access log entry.
     * The table name (date-based) is assigned automatically.
     */
    public void add(RestAccessLogModel model) {
        batchService.add(model);
    }

    /**
     * Enqueues a general access log entry.
     * The table name (date+hour-based) is assigned automatically.
     */
    public void addAccessLog(AccessLogModel model) {
        batchService.addAccessLog(model);
    }

    /**
     * Flushes any remaining buffered entries and shuts down internal thread pools.
     * Call once on application shutdown.
     */
    public void shutdown() {
        batchService.shutdown();
    }
}
