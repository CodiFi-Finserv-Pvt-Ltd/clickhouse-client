package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.EmailLogModel;
import in.codifi.clickhouse.logs.model.OrderDetailModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.SmsLogModel;
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
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertRestAccessLogBatch failed for table {} — {} rows ({})",
                    tableName, logs.size(), errorType(e));
            log.debug("insertRestAccessLogBatch failure detail for table {}", tableName, e);
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
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == batchLogs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            statement.executeBatch();

        } catch (Exception e) {
            log.error("insertAccessLogBatch failed for table {} — {} rows ({})",
                    tableName, batchLogs.size(), errorType(e));
            log.debug("insertAccessLogBatch failure detail for table {}", tableName, e);
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
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertLoginReportBatch failed for table {} — {} rows ({})",
                    tableName, logs.size(), errorType(e));
            log.debug("insertLoginReportBatch failure detail for table {}", tableName, e);
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

    // -----------------------------------------------------------------
    // tbl_email_logs  — 13 INSERT cols; created_on defaults in ClickHouse
    //
    // CREATE order (insertable):
    //  1  user_id        6  email_from       11 created_by
    //  2  email          7  email_subject    12 source
    //  3  otp            8  email_content    13 filler
    //  4  email_host     9  email_response
    //  5  email_port    10  email_status
    // -----------------------------------------------------------------
    public void insertEmailLogBatch(String tableName, List<EmailLogModel> logs) {
        if (logs == null || logs.isEmpty()) return;

        String query = "INSERT INTO " + tableName
                + " (user_id, email, otp, email_host, email_port, email_from, email_subject,"
                + " email_content, email_response, email_status, created_by, source, filler)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            int failedRows = 0;
            for (EmailLogModel entry : logs) {
                try {
                    bindEmailLogRow(ps, tableName, entry);
                    ps.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertEmailLogBatch failed for table {} — {} rows ({})",
                    tableName, logs.size(), errorType(e));
            log.debug("insertEmailLogBatch failure detail for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindEmailLogRow(PreparedStatement ps, String tableName, EmailLogModel entry)
            throws Exception {
        int i = 1;
        SafeBind.bindString(ps, i++, entry::getUserId, tableName, "user_id");                 //  1
        SafeBind.bindNullableString(ps, i++, entry::getEmail, tableName, "email");             //  2
        SafeBind.bindNullableString(ps, i++, () -> MaskingUtil.maskOtp(entry.getOtp()), tableName, "otp"); //  3
        SafeBind.bindNullableString(ps, i++, entry::getEmailHost, tableName, "email_host");    //  4
        SafeBind.bindNullableString(ps, i++, entry::getEmailPort, tableName, "email_port");    //  5
        SafeBind.bindNullableString(ps, i++, entry::getEmailFrom, tableName, "email_from");    //  6
        SafeBind.bindString(ps, i++, entry::getEmailSubject, tableName, "email_subject");      //  7
        SafeBind.bindString(ps, i++, entry::getEmailContent, tableName, "email_content");      //  8
        SafeBind.bindString(ps, i++, entry::getEmailResponse, tableName, "email_response");    //  9
        SafeBind.bindNullableString(ps, i++, entry::getEmailStatus, tableName, "email_status"); // 10
        SafeBind.bindNullableString(ps, i++, entry::getCreatedBy, tableName, "created_by");    // 11
        SafeBind.bindNullableString(ps, i++, entry::getSource, tableName, "source");           // 12
        SafeBind.bindNullableString(ps, i++, entry::getFiller, tableName, "filler");           // 13
    }

    // -----------------------------------------------------------------
    // tbl_sms_logs  — 10 INSERT cols; created_on defaults in ClickHouse
    //
    // CREATE order (insertable):
    //  1  user_id    5  request_message    9  source
    //  2  mobile     6  response_message  10  filler
    //  3  otp        7  sent_status
    //  4  url        8  created_by
    // -----------------------------------------------------------------
    public void insertSmsLogBatch(String tableName, List<SmsLogModel> logs) {
        if (logs == null || logs.isEmpty()) return;

        String query = "INSERT INTO " + tableName
                + " (user_id, mobile, otp, url, request_message, response_message,"
                + " sent_status, created_by, source, filler)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            int failedRows = 0;
            for (SmsLogModel entry : logs) {
                try {
                    bindSmsLogRow(ps, tableName, entry);
                    ps.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertSmsLogBatch failed for table {} — {} rows ({})",
                    tableName, logs.size(), errorType(e));
            log.debug("insertSmsLogBatch failure detail for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindSmsLogRow(PreparedStatement ps, String tableName, SmsLogModel entry)
            throws Exception {
        int i = 1;
        SafeBind.bindString(ps, i++, entry::getUserId, tableName, "user_id");                       //  1
        SafeBind.bindNullableString(ps, i++, entry::getMobile, tableName, "mobile");                 //  2
        SafeBind.bindNullableString(ps, i++, () -> MaskingUtil.maskOtp(entry.getOtp()), tableName, "otp"); //  3
        SafeBind.bindString(ps, i++, entry::getUrl, tableName, "url");                               //  4
        SafeBind.bindString(ps, i++, entry::getRequestMessage, tableName, "request_message");        //  5
        SafeBind.bindString(ps, i++, entry::getResponseMessage, tableName, "response_message");      //  6
        SafeBind.bindNullableString(ps, i++, entry::getSentStatus, tableName, "sent_status");        //  7
        SafeBind.bindNullableString(ps, i++, entry::getCreatedBy, tableName, "created_by");          //  8
        SafeBind.bindNullableString(ps, i++, entry::getSource, tableName, "source");                 //  9
        SafeBind.bindNullableString(ps, i++, entry::getFiller, tableName, "filler");                 // 10
    }

    // -----------------------------------------------------------------
    // tbl_order_details  — 51 INSERT cols; created_on defaults in ClickHouse
    //
    // All model fields are String. Decimal(18,6) / Int8 columns are parsed by
    // SafeBind; a null/blank/unparseable/out-of-range value binds as NULL.
    // Column list is backtick-quoted (contains `user`).
    // -----------------------------------------------------------------
    public void insertOrderDetailBatch(String tableName, List<OrderDetailModel> logs) {
        if (logs == null || logs.isEmpty()) return;

        String query = "INSERT INTO " + tableName
                + " (`user_id`, `acc_id`, `user`, `token`, `exchange`, `trading_symbol`, `action`,"
                + " `trans_type`, `product`, `validity`, `prc_type`, `order_type`, `customer_firm`,"
                + " `qty`, `price`, `trigger_price`, `disc_qty`, `cancelled_size`, `mkt_protection_price`,"
                + " `order_no`, `target`, `stop_loss_value`, `trailing_stop_loss`, `tick_size`,"
                + " `ltp_or_atp`, `sqr_off_abs_or_tick`, `slabs_or_tick`, `trail_tick_yes_no`,"
                + " `classification`, `vendor_code`, `flag_to_add_category`, `order_criteria`,"
                + " `pos_sqr_off_flag`, `algo_id`, `order_tag`, `comments`, `source`, `method`, `module`,"
                + " `device_ip`, `device_id`, `status`, `rej_reason`, `active_status`,"
                + " `created_by`, `app_code`, `source_platform`, `filler1`, `filler2`, `filler3`, `filler4`)"
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            int failedRows = 0;
            for (OrderDetailModel entry : logs) {
                try {
                    bindOrderDetailRow(ps, tableName, entry);
                    ps.addBatch();
                } catch (Exception rowEx) {
                    failedRows++;
                    log.warn("Skipping unmappable row for {} ({})", tableName, errorType(rowEx));
                    log.debug("Unmappable row detail for {}", tableName, rowEx);
                }
            }
            if (failedRows == logs.size()) {
                throw new RuntimeException("All " + failedRows + " rows failed to bind for " + tableName);
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertOrderDetailBatch failed for table {} — {} rows ({})",
                    tableName, logs.size(), errorType(e));
            log.debug("insertOrderDetailBatch failure detail for table {}", tableName, e);
            throw new RuntimeException(e);
        }
    }

    private void bindOrderDetailRow(PreparedStatement ps, String t, OrderDetailModel e) throws Exception {
        int i = 1;
        SafeBind.bindString(ps, i++, e::getUserId, t, "user_id");                                     //  1
        SafeBind.bindNullableString(ps, i++, e::getAccId, t, "acc_id");                               //  2
        SafeBind.bindNullableString(ps, i++, e::getUser, t, "user");                                  //  3
        SafeBind.bindNullableString(ps, i++, e::getToken, t, "token");                                //  4
        SafeBind.bindNullableString(ps, i++, e::getExchange, t, "exchange");                          //  5
        SafeBind.bindNullableString(ps, i++, e::getTradingSymbol, t, "trading_symbol");               //  6
        SafeBind.bindNullableString(ps, i++, e::getAction, t, "action");                              //  7
        SafeBind.bindNullableString(ps, i++, e::getTransType, t, "trans_type");                       //  8
        SafeBind.bindNullableString(ps, i++, e::getProduct, t, "product");                            //  9
        SafeBind.bindNullableString(ps, i++, e::getValidity, t, "validity");                          // 10
        SafeBind.bindNullableString(ps, i++, e::getPrcType, t, "prc_type");                           // 11
        SafeBind.bindNullableString(ps, i++, e::getOrderType, t, "order_type");                       // 12
        SafeBind.bindNullableString(ps, i++, e::getCustomerFirm, t, "customer_firm");                 // 13
        SafeBind.bindNullableDecimal(ps, i++, e::getQty, t, "qty", 18, 6);                            // 14
        SafeBind.bindNullableDecimal(ps, i++, e::getPrice, t, "price", 18, 6);                        // 15
        SafeBind.bindNullableDecimal(ps, i++, e::getTriggerPrice, t, "trigger_price", 18, 6);         // 16
        SafeBind.bindNullableDecimal(ps, i++, e::getDiscQty, t, "disc_qty", 18, 6);                   // 17
        SafeBind.bindNullableDecimal(ps, i++, e::getCancelledSize, t, "cancelled_size", 18, 6);       // 18
        SafeBind.bindNullableDecimal(ps, i++, e::getMktProtectionPrice, t, "mkt_protection_price", 18, 6); // 19
        SafeBind.bindNullableString(ps, i++, e::getOrderNo, t, "order_no");                           // 20
        SafeBind.bindNullableDecimal(ps, i++, e::getTarget, t, "target", 18, 6);                      // 21
        SafeBind.bindNullableDecimal(ps, i++, e::getStopLossValue, t, "stop_loss_value", 18, 6);      // 22
        SafeBind.bindNullableDecimal(ps, i++, e::getTrailingStopLoss, t, "trailing_stop_loss", 18, 6); // 23
        SafeBind.bindNullableDecimal(ps, i++, e::getTickSize, t, "tick_size", 18, 6);                 // 24
        SafeBind.bindNullableString(ps, i++, e::getLtpOrAtp, t, "ltp_or_atp");                        // 25
        SafeBind.bindNullableString(ps, i++, e::getSqrOffAbsOrTick, t, "sqr_off_abs_or_tick");        // 26
        SafeBind.bindNullableString(ps, i++, e::getSlabsOrTick, t, "slabs_or_tick");                  // 27
        SafeBind.bindNullableString(ps, i++, e::getTrailTickYesNo, t, "trail_tick_yes_no");           // 28
        SafeBind.bindNullableString(ps, i++, e::getClassification, t, "classification");              // 29
        SafeBind.bindNullableString(ps, i++, e::getVendorCode, t, "vendor_code");                     // 30
        SafeBind.bindNullableString(ps, i++, e::getFlagToAddCategory, t, "flag_to_add_category");     // 31
        SafeBind.bindNullableString(ps, i++, e::getOrderCriteria, t, "order_criteria");               // 32
        SafeBind.bindNullableString(ps, i++, e::getPosSqrOffFlag, t, "pos_sqr_off_flag");             // 33
        SafeBind.bindNullableString(ps, i++, e::getAlgoId, t, "algo_id");                             // 34
        SafeBind.bindNullableString(ps, i++, e::getOrderTag, t, "order_tag");                         // 35
        SafeBind.bindNullableString(ps, i++, e::getComments, t, "comments");                          // 36
        SafeBind.bindNullableString(ps, i++, e::getSource, t, "source");                              // 37
        SafeBind.bindNullableString(ps, i++, e::getMethod, t, "method");                              // 38
        SafeBind.bindNullableString(ps, i++, e::getModule, t, "module");                              // 39
        SafeBind.bindNullableString(ps, i++, e::getDeviceIp, t, "device_ip");                         // 40
        SafeBind.bindNullableString(ps, i++, e::getDeviceId, t, "device_id");                         // 41
        SafeBind.bindNullableString(ps, i++, e::getStatus, t, "status");                              // 42
        SafeBind.bindNullableString(ps, i++, e::getRejReason, t, "rej_reason");                       // 43
        SafeBind.bindNullableInt8(ps, i++, e::getActiveStatus, t, "active_status");                   // 44
        // created_on DEFAULT now() — skipped
        SafeBind.bindNullableString(ps, i++, e::getCreatedBy, t, "created_by");                       // 45
        SafeBind.bindNullableString(ps, i++, e::getAppCode, t, "app_code");                           // 46
        SafeBind.bindNullableString(ps, i++, e::getSourcePlatform, t, "source_platform");             // 47
        SafeBind.bindNullableString(ps, i++, e::getFiller1, t, "filler1");                            // 48
        SafeBind.bindNullableString(ps, i++, e::getFiller2, t, "filler2");                            // 49
        SafeBind.bindNullableString(ps, i++, e::getFiller3, t, "filler3");                            // 50
        SafeBind.bindNullableString(ps, i++, e::getFiller4, t, "filler4");                            // 51
    }

    /**
     * Root-cause class name only. Driver/server messages can quote the INSERT text (i.e. row
     * values), so failure logs use this instead of the exception message or stack trace.
     */
    public static String errorType(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root.getClass().getSimpleName();
    }
}
