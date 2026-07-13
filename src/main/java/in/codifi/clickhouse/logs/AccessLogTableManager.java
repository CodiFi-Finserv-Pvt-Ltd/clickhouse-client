package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLogTableManager {

    private static final Logger log = LoggerFactory.getLogger(AccessLogTableManager.class);

    private final DataSource ds;
    private final String     database;

    public AccessLogTableManager(DataSource ds, String database) {
        this.ds       = ds;
        this.database = database;
    }

    /**
     * Creates (or confirms) both fixed partitioned tables.
     * Returns true if both succeeded, false if either failed.
     */
    public boolean ensureTables() {
        boolean ok = true;
        ok &= execute(buildAccessLogDdl(), database + ".tbl_access_log");
        ok &= execute(buildRestAccessLogDdl(), database + ".tbl_rest_access_log");
        return ok;
    }

    private String buildAccessLogDdl() {
        return "CREATE TABLE IF NOT EXISTS " + database + ".tbl_access_log ("
                + "user_id String, "
                + "ucc Nullable(String), "
                + "uri String, "
                + "module LowCardinality(Nullable(String)), "
                + "method LowCardinality(Nullable(String)), "
                + "status_code Nullable(Int32), "
                + "source LowCardinality(Nullable(String)), "
                + "req_body String CODEC(ZSTD(3)), "
                + "res_body String CODEC(ZSTD(3)), "
                + "device_ip Nullable(String), "
                + "device_id Nullable(String), "
                + "req_id Nullable(String), "
                + "user_agent Nullable(String), "
                + "domain Nullable(String), "
                + "content_type LowCardinality(Nullable(String)), "
                + "session Nullable(String), "
                + "in_time DateTime64(3), "
                + "out_time DateTime64(3), "
                + "latency_ms Int64 MATERIALIZED dateDiff('millisecond', in_time, out_time), "
                + "vendor LowCardinality(Nullable(String)), "
                + "filler1 Nullable(String), "
                + "filler2 Nullable(String), "
                + "pod_id Nullable(String)"
                + ") ENGINE = MergeTree"
                + " PARTITION BY toYYYYMMDD(in_time)"
                + " ORDER BY (user_id, in_time, module, uri)"
                + " SETTINGS index_granularity = 8192, allow_nullable_key = 1";
    }

    private String buildRestAccessLogDdl() {
        return "CREATE TABLE IF NOT EXISTS " + database + ".tbl_rest_access_log ("
                + "user_id String, "
                + "ucc Nullable(String), "
                + "module LowCardinality(Nullable(String)), "
                + "device_id Nullable(String), "
                + "method LowCardinality(Nullable(String)), "
                + "status_code Nullable(Int32), "
                + "url String, "
                + "req_id Nullable(String), "
                + "req_body String CODEC(ZSTD(3)), "
                + "res_body String CODEC(ZSTD(3)), "
                + "in_time DateTime64(3), "
                + "out_time DateTime64(3), "
                + "latency_ms Int64 MATERIALIZED dateDiff('millisecond', in_time, out_time), "
                + "total_time Nullable(UInt64), "
                + "vendor LowCardinality(Nullable(String)), "
                + "source LowCardinality(Nullable(String)), "
                + "filler1 Nullable(String), "
                + "filler2 Nullable(String), "
                + "pod_id Nullable(String)"
                + ") ENGINE = MergeTree"
                + " PARTITION BY toYYYYMMDD(in_time)"
                + " ORDER BY (user_id, in_time, module)"
                + " SETTINGS index_granularity = 8192, allow_nullable_key = 1";
    }

    private boolean execute(String sql, String label) {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return true;
        } catch (Exception e) {
            log.error("Failed to create table {}: {}", label, e.getMessage());
            return false;
        }
    }
}
