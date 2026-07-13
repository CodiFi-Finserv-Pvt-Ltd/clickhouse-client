package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.util.CommonUtils;
import in.codifi.clickhouse.logs.util.MaskingUtil;

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

            for (RestAccessLogModel entry : logs) {
                int i = 1;

                Timestamp now = new Timestamp(System.currentTimeMillis());
                Timestamp inT  = entry.getInTime()  != null ? entry.getInTime()  : now;
                Timestamp outT = entry.getOutTime() != null ? entry.getOutTime() : inT;

                ps.setString(i++, nvl(entry.getUserId()));                   //  1  user_id
                setNullableString(ps, i++, entry.getUcc());                  //  2  ucc
                setNullableString(ps, i++, entry.getModule());               //  3  module
                setNullableString(ps, i++, entry.getDeviceId());             //  4  device_id
                setNullableString(ps, i++, entry.getMethod());               //  5  method
                setNullableInt(ps, i++, entry.getStatusCode());              //  6  status_code
                ps.setString(i++, nvl(entry.getUrl()));                      //  7  url
                setNullableString(ps, i++, entry.getReqId());                //  8  req_id

                String req = entry.getReqBody();
                if (maskingEnabled && req != null) req = MaskingUtil.maskJson(req);
                ps.setString(i++, req != null ? req : "");                   //  9  req_body

                String res = entry.getResBody();
                if (maskingEnabled && res != null) res = MaskingUtil.maskJson(res);
                ps.setString(i++, res != null ? res : "");                   // 10  res_body

                ps.setTimestamp(i++, inT);                                   // 11  in_time
                ps.setTimestamp(i++, outT);                                  // 12  out_time
                // latency_ms MATERIALIZED — skipped

                setNullableLong(ps, i++, entry.getTotalTime());              // 13  total_time
                setNullableString(ps, i++, entry.getVendor());               // 14  vendor
                setNullableString(ps, i++, entry.getSource());               // 15  source
                setNullableString(ps, i++, entry.getFiller1());              // 16  filler1
                setNullableString(ps, i++, entry.getFiller2());              // 17  filler2
                setNullableString(ps, i++, entry.getPodId());                // 18  pod_id

                ps.addBatch();
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertRestAccessLogBatch failed for table {}", tableName, e);
            throw new RuntimeException(e);
        }
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

            for (AccessLogModel m : batchLogs) {
                int i = 1;

                Timestamp now = new Timestamp(System.currentTimeMillis());
                Timestamp inT  = m.getInTime()  != null ? m.getInTime()  : now;
                Timestamp outT = m.getOutTime() != null ? m.getOutTime() : inT;

                statement.setString(i++, nvl(m.getUserId()));                //  1  user_id
                setNullableString(statement, i++, m.getUcc());               //  2  ucc
                statement.setString(i++, nvl(m.getUri()));                   //  3  uri
                setNullableString(statement, i++, m.getModule());            //  4  module
                setNullableString(statement, i++, m.getMethod());            //  5  method
                setNullableInt(statement, i++, m.getStatusCode());           //  6  status_code
                setNullableString(statement, i++, m.getSource());            //  7  source

                String reqBody = m.getReqBody();
                if (maskingEnabled && reqBody != null) reqBody = MaskingUtil.maskJson(reqBody);
                statement.setString(i++, reqBody != null ? reqBody : "");    //  8  req_body

                String resBody = m.getResBody();
                if (maskingEnabled && resBody != null) resBody = MaskingUtil.maskJson(resBody);
                statement.setString(i++, resBody != null ? resBody : "");   //  9  res_body

                setNullableString(statement, i++, m.getDeviceIp());          // 10  device_ip
                setNullableString(statement, i++, m.getDeviceId());          // 11  device_id
                setNullableString(statement, i++, m.getReqId());             // 12  req_id
                setNullableString(statement, i++, m.getUserAgent());         // 13  user_agent
                setNullableString(statement, i++, m.getDomain());            // 14  domain
                setNullableString(statement, i++, m.getContentType());       // 15  content_type
                setNullableString(statement, i++, m.getSession());           // 16  session

                statement.setTimestamp(i++, inT);                            // 17  in_time
                statement.setTimestamp(i++, outT);                           // 18  out_time
                // latency_ms MATERIALIZED — skipped

                setNullableString(statement, i++, m.getVendor());            // 19  vendor
                setNullableString(statement, i++, m.getFiller1());           // 20  filler1
                setNullableString(statement, i++, m.getFiller2());           // 21  filler2
                setNullableString(statement, i++, m.getPodId());             // 22  pod_id

                statement.addBatch();
            }

            statement.executeBatch();

        } catch (Exception e) {
            log.error("insertAccessLogBatch failed for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    // ---- helpers ----

    private static String nvl(String val) {
        return val != null ? val : "";
    }

    private static void setNullableString(PreparedStatement ps, int idx, String val) throws Exception {
        if (val != null) ps.setString(idx, val);
        else             ps.setNull(idx, Types.VARCHAR);
    }

    private static void setNullableInt(PreparedStatement ps, int idx, Integer val) throws Exception {
        if (val != null) ps.setInt(idx, val);
        else             ps.setNull(idx, Types.INTEGER);
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long val) throws Exception {
        if (val != null) ps.setLong(idx, val);
        else             ps.setNull(idx, Types.BIGINT);
    }
}
