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
        ok &= execute(buildUserLoginReportDdl(), database + ".tbl_user_loggedin_report");
        ok &= execute(buildEmailLogDdl(), database + ".tbl_email_logs");
        ok &= execute(buildSmsLogDdl(), database + ".tbl_sms_logs");
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

    private String buildUserLoginReportDdl() {
        return "CREATE TABLE IF NOT EXISTS " + database + ".tbl_user_loggedin_report ("
                + "`user_id` String, "
                + "`source` LowCardinality(String), "
                + "`vendor` LowCardinality(Nullable(String)), "
                + "`visitors` Int32, "
                + "`report_date` Date DEFAULT toDate(`created_on`), "
                + "`created_on` DateTime DEFAULT now()"
                + ") ENGINE = SummingMergeTree(`visitors`)"
                + " PARTITION BY toYYYYMM(`report_date`)"
                + " ORDER BY (`report_date`, `user_id`, `source`)"
                + " SETTINGS index_granularity = 8192";
    }

    private String buildEmailLogDdl() {
        return "CREATE TABLE IF NOT EXISTS " + database + ".tbl_email_logs ("
                + "`user_id` String, "
                + "`email` Nullable(String), "
                + "`otp` Nullable(String), "
                + "`email_host` LowCardinality(Nullable(String)), "
                + "`email_port` LowCardinality(Nullable(String)), "
                + "`email_from` LowCardinality(Nullable(String)), "
                + "`email_subject` String CODEC(ZSTD(3)), "
                + "`email_content` String CODEC(ZSTD(3)), "
                + "`email_response` String CODEC(ZSTD(3)), "
                + "`email_status` LowCardinality(Nullable(String)), "
                + "`created_on` DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)), "
                + "`created_by` LowCardinality(Nullable(String)), "
                + "`source` LowCardinality(Nullable(String)), "
                + "`filler` Nullable(String)"
                + ") ENGINE = MergeTree"
                + " PARTITION BY toYYYYMM(`created_on`)"
                + " ORDER BY (`created_on`, `user_id`)"
                + " SETTINGS index_granularity = 8192, allow_nullable_key = 1";
    }

    private String buildSmsLogDdl() {
        return "CREATE TABLE IF NOT EXISTS " + database + ".tbl_sms_logs ("
                + "`user_id` String, "
                + "`mobile` Nullable(String), "
                + "`otp` Nullable(String), "
                + "`url` String CODEC(ZSTD(3)), "
                + "`request_message` String CODEC(ZSTD(3)), "
                + "`response_message` String CODEC(ZSTD(3)), "
                + "`sent_status` LowCardinality(Nullable(String)), "
                + "`created_on` DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)), "
                + "`created_by` LowCardinality(Nullable(String)), "
                + "`source` LowCardinality(Nullable(String)), "
                + "`filler` Nullable(String)"
                + ") ENGINE = MergeTree"
                + " PARTITION BY toYYYYMM(`created_on`)"
                + " ORDER BY (`created_on`, `user_id`)"
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
