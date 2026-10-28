package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;
import in.codifi.clickhouse.logs.util.CommonUtils;
import in.codifi.clickhouse.logs.util.MaskingUtil;
import in.codifi.clickhouse.logs.util.SafeBind;

public class AccessLogWriter {

    private static final Logger log = LoggerFactory.getLogger(AccessLogWriter.class);

    private final DataSource ds;
    private final boolean    maskingEnabled;

    // Legacy constructors kept for binary/source compatibility; commonUtils is no longer used.
    public AccessLogWriter(DataSource ds, CommonUtils commonUtils) {
        this(ds, commonUtils, false);
    }

    public AccessLogWriter(DataSource ds, CommonUtils commonUtils, boolean maskingEnabled) {
        this.ds             = ds;
        this.maskingEnabled = maskingEnabled;
    }

    public AccessLogWriter(DataSource ds, boolean maskingEnabled) {
        this.ds             = ds;
        this.maskingEnabled = maskingEnabled;
    }

    // -----------------------------------------------------------------
    // tbl_rest_access_log  — 18 INSERT cols; latency_ms is MATERIALIZED
    //
    // CREATE order (insertable):
    //  1  user_id          6  status_code   11 in_time
    //  2  ucc              7  url           12 out_time
    //  3  module           8  req_id        13 total_time
    //  4  device_id        9  req_body      14 vendor
    //  5  method           10 res_body      15 source
    //                                       16 filler1
    //                                       17 filler2
    //                                       18 pod_id
    // -----------------------------------------------------------------
    public void insertRestAccessLogBatch(String tableName, List<RestAccessLogModel> logs) {
        if (logs == null || logs.isEmpty()) return;

        String query = "INSERT INTO " + tableName
                + " (user_id, ucc, module, device_id, method, status_code, url,"
                + " req_id, req_body, res_body, in_time, out_time,"
                + " total_time, vendor, source, filler1, filler2, pod_id)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            int failedRows = 0;
            for (RestAccessLogModel entry : logs) {
                try {
                    bindRestAccessLogRow(ps, tableName, entry);
                    ps.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {}: {}", tableName, rowEx.getMessage());
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertRestAccessLogBatch failed for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindRestAccessLogRow(PreparedStatement ps, String tableName, RestAccessLogModel entry)
            throws Exception {
        int i = 1;

        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp inT  = entry.getInTime()  != null ? entry.getInTime()  : now;
        Timestamp outT = entry.getOutTime() != null ? entry.getOutTime() : inT;

        SafeBind.bindString(ps, i++, entry::getUserId, tableName, "user_id");                 //  1
        SafeBind.bindNullableString(ps, i++, entry::getUcc, tableName, "ucc");                 //  2
        SafeBind.bindNullableString(ps, i++, entry::getModule, tableName, "module");           //  3
        SafeBind.bindNullableString(ps, i++, entry::getDeviceId, tableName, "device_id");       //  4
        SafeBind.bindNullableString(ps, i++, entry::getMethod, tableName, "method");            //  5
        SafeBind.bindNullableInt(ps, i++, entry::getStatusCode, tableName, "status_code");      //  6
        SafeBind.bindString(ps, i++, entry::getUrl, tableName, "url");                          //  7
        SafeBind.bindNullableString(ps, i++, entry::getReqId, tableName, "req_id");             //  8

        String req = entry.getReqBody();
        if (maskingEnabled && req != null) req = MaskingUtil.maskJson(req);
        final String reqFinal = req;
        SafeBind.bindString(ps, i++, () -> reqFinal, tableName, "req_body");                    //  9

        String res = entry.getResBody();
        if (maskingEnabled && res != null) res = MaskingUtil.maskJson(res);
        final String resFinal = res;
        SafeBind.bindString(ps, i++, () -> resFinal, tableName, "res_body");                     // 10

        ps.setTimestamp(i++, inT);                                    // 11  in_time
        ps.setTimestamp(i++, outT);                                   // 12  out_time
        // latency_ms MATERIALIZED — skipped

        SafeBind.bindNullableLong(ps, i++, entry::getTotalTime, tableName, "total_time");        // 13
        SafeBind.bindNullableString(ps, i++, entry::getVendor, tableName, "vendor");             // 14
        SafeBind.bindNullableString(ps, i++, entry::getSource, tableName, "source");             // 15
        SafeBind.bindNullableString(ps, i++, entry::getFiller1, tableName, "filler1");           // 16
        SafeBind.bindNullableString(ps, i++, entry::getFiller2, tableName, "filler2");           // 17
        SafeBind.bindNullableString(ps, i++, entry::getPodId, tableName, "pod_id");              // 18
    }

    // -----------------------------------------------------------------
    // tbl_access_log  — 22 INSERT cols; latency_ms is MATERIALIZED
    //
    // CREATE order (insertable):
    //  1  user_id          9  res_body      17 in_time
    //  2  ucc             10  device_ip     18 out_time
    //  3  uri             11  device_id     19 vendor
    //  4  module          12  req_id        20 filler1
    //  5  method          13  user_agent    21 filler2
    //  6  status_code     14  domain        22 pod_id
    //  7  source          15  content_type
    //  8  req_body        16  session
    // -----------------------------------------------------------------
    public void insertAccessLogBatch(String tableName, List<AccessLogModel> batchLogs) {
        if (batchLogs == null || batchLogs.isEmpty()) return;

        String insertQuery = "INSERT INTO " + tableName
                + " (user_id, ucc, uri, module, method, status_code, source,"
                + " req_body, res_body, device_ip, device_id, req_id,"
                + " user_agent, domain, content_type, session,"
                + " in_time, out_time,"
                + " vendor, filler1, filler2, pod_id)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection connection = ds.getConnection();
                PreparedStatement statement = connection.prepareStatement(insertQuery)) {

            int failedRows = 0;
            for (AccessLogModel m : batchLogs) {
                try {
                    bindAccessLogRow(statement, tableName, m);
                    statement.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {}: {}", tableName, rowEx.getMessage());
                }
            }
            if (failedRows == batchLogs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            statement.executeBatch();

        } catch (Exception e) {
            log.error("insertAccessLogBatch failed for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindAccessLogRow(PreparedStatement statement, String tableName, AccessLogModel m)
            throws Exception {
        int i = 1;

        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp inT  = m.getInTime()  != null ? m.getInTime()  : now;
        Timestamp outT = m.getOutTime() != null ? m.getOutTime() : inT;

        SafeBind.bindString(statement, i++, m::getUserId, tableName, "user_id");                //  1
        SafeBind.bindNullableString(statement, i++, m::getUcc, tableName, "ucc");                //  2
        SafeBind.bindString(statement, i++, m::getUri, tableName, "uri");                        //  3
        SafeBind.bindNullableString(statement, i++, m::getModule, tableName, "module");          //  4
        SafeBind.bindNullableString(statement, i++, m::getMethod, tableName, "method");          //  5
        SafeBind.bindNullableInt(statement, i++, m::getStatusCode, tableName, "status_code");     //  6
        SafeBind.bindNullableString(statement, i++, m::getSource, tableName, "source");           //  7

        String reqBody = m.getReqBody();
        if (maskingEnabled && reqBody != null) reqBody = MaskingUtil.maskJson(reqBody);
        final String reqFinal = reqBody;
        SafeBind.bindString(statement, i++, () -> reqFinal, tableName, "req_body");               //  8

        String resBody = m.getResBody();
        if (maskingEnabled && resBody != null) resBody = MaskingUtil.maskJson(resBody);
        final String resFinal = resBody;
        SafeBind.bindString(statement, i++, () -> resFinal, tableName, "res_body");                //  9

        SafeBind.bindNullableString(statement, i++, m::getDeviceIp, tableName, "device_ip");       // 10
        SafeBind.bindNullableString(statement, i++, m::getDeviceId, tableName, "device_id");       // 11
        SafeBind.bindNullableString(statement, i++, m::getReqId, tableName, "req_id");             // 12
        SafeBind.bindNullableString(statement, i++, m::getUserAgent, tableName, "user_agent");     // 13
        SafeBind.bindNullableString(statement, i++, m::getDomain, tableName, "domain");            // 14
        SafeBind.bindNullableString(statement, i++, m::getContentType, tableName, "content_type"); // 15
        SafeBind.bindNullableString(statement, i++, m::getSession, tableName, "session");          // 16

        statement.setTimestamp(i++, inT);                              // 17  in_time
        statement.setTimestamp(i++, outT);                             // 18  out_time
        // latency_ms MATERIALIZED — skipped

        SafeBind.bindNullableString(statement, i++, m::getVendor, tableName, "vendor");            // 19
        SafeBind.bindNullableString(statement, i++, m::getFiller1, tableName, "filler1");           // 20
        SafeBind.bindNullableString(statement, i++, m::getFiller2, tableName, "filler2");           // 21
        SafeBind.bindNullableString(statement, i++, m::getPodId, tableName, "pod_id");              // 22
    }

    // -----------------------------------------------------------------
    // tbl_user_loggedin_report  — 4 INSERT cols; report_date/created_on default in ClickHouse
    //
    // CREATE order (insertable):
    //  1  user_id   2  source   3  vendor   4  visitors
    // -----------------------------------------------------------------
    public void insertLoginReportBatch(String tableName, List<UserLoginReportModel> logs) {
        if (logs == null || logs.isEmpty()) return;

        String query = "INSERT INTO " + tableName
                + " (user_id, source, vendor, visitors)"
                + " VALUES (?,?,?,?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            int failedRows = 0;
            for (UserLoginReportModel entry : logs) {
                try {
                    bindLoginReportRow(ps, tableName, entry);
                    ps.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {}: {}", tableName, rowEx.getMessage());
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertLoginReportBatch failed for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindLoginReportRow(PreparedStatement ps, String tableName, UserLoginReportModel entry)
            throws Exception {
        int i = 1;
        SafeBind.bindString(ps, i++, entry::getUserId, tableName, "user_id");           //  1
        SafeBind.bindString(ps, i++, entry::getSource, tableName, "source");            //  2
        SafeBind.bindNullableString(ps, i++, entry::getVendor, tableName, "vendor");     //  3
        SafeBind.bindInt(ps, i++, entry::getVisitors, tableName, "visitors");            //  4
    }
}
