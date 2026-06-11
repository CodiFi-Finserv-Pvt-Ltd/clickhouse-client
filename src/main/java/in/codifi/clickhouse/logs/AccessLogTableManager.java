package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLogTableManager {

    private static final Logger log = LoggerFactory.getLogger(AccessLogTableManager.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("ddMMyyyy");

    private final DataSource ds;
    private final String     database;

    public AccessLogTableManager(DataSource ds, String database) {
        this.ds       = ds;
        this.database = database;
    }

    public void ensureTables() {
        LocalDate today = LocalDate.now();
        for (int d = 0; d <= 7; d++) {
            LocalDate day      = today.plusDays(d);
            String    datePart = day.format(DATE_FMT);

            for (int h = 0; h < 24; h++) {
                String hour      = String.format("%02d", h);
                String tableName = "tbl_" + datePart + "_access_log_" + hour;
                execute(buildAccessLogDdl(tableName), tableName);
            }

            String restTable = "tbl_" + datePart + "_rest_access_log";
            execute(buildRestAccessLogDdl(restTable), restTable);
        }
    }

    private String buildAccessLogDdl(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + database + "." + tableName + " ("
                + "id UUID DEFAULT generateUUIDv4(), "
                + "user_id Nullable(String), "
                + "ucc Nullable(String), "
                + "uri Nullable(String), "
                + "module Nullable(String), "
                + "method Nullable(String), "
                + "source Nullable(String), "
                + "req_body Nullable(String), "
                + "res_body Nullable(String), "
                + "device_ip Nullable(String), "
                + "device_id Nullable(String), "
                + "req_id Nullable(String), "
                + "user_agent Nullable(String), "
                + "domain Nullable(String), "
                + "content_type Nullable(String), "
                + "session Nullable(String), "
                + "in_time Nullable(String), "
                + "out_time Nullable(String), "
                + "lag_time Nullable(Int64), "
                + "elapsed_time Nullable(String), "
                + "vendor Nullable(String), "
                + "created_on Nullable(String)"
                + ") ENGINE = MergeTree ORDER BY (user_id, created_on) "
                + "SETTINGS index_granularity = 8192, allow_nullable_key = 1";
    }

    private String buildRestAccessLogDdl(String tableName) {
        return "CREATE TABLE IF NOT EXISTS " + database + "." + tableName + " ("
                + "id UUID DEFAULT generateUUIDv4(), "
                + "user_id Nullable(String), "
                + "module Nullable(String), "
                + "device_id Nullable(String), "
                + "method Nullable(String), "
                + "url Nullable(String), "
                + "req_body Nullable(String), "
                + "res_body Nullable(String), "
                + "in_time Nullable(String), "
                + "out_time Nullable(String), "
                + "total_time Nullable(UInt64), "
                + "created_on Nullable(String), "
                + "vendor Nullable(String), "
                + "source Nullable(String)"
                + ") ENGINE = MergeTree ORDER BY (user_id, created_on) "
                + "SETTINGS index_granularity = 8192, allow_nullable_key = 1";
    }

    private void execute(String sql, String tableName) {
        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception e) {
            log.error("Failed to create table {}: {}", tableName, e.getMessage());
        }
    }
}
