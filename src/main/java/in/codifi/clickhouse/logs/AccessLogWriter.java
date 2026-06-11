package in.codifi.clickhouse.logs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.util.CommonUtils;
import in.codifi.clickhouse.logs.util.MaskingUtil;
import in.codifi.clickhouse.logs.util.StringUtil;

public class AccessLogWriter {

    private static final Logger log = LoggerFactory.getLogger(AccessLogWriter.class);

    private final DataSource  ds;
    private final CommonUtils commonUtils;

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public AccessLogWriter(DataSource ds, CommonUtils commonUtils) {
        this.ds          = ds;
        this.commonUtils = commonUtils;
    }

    public void insertRestAccessLogBatch(String tableName, List<RestAccessLogModel> logs) {

        if (logs == null || logs.isEmpty())
            return;

        String query = "INSERT INTO " + tableName
                + " (user_id, url, in_time, out_time, total_time, module, method, req_body, res_body, vendor, created_on) "
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(query)) {

            for (RestAccessLogModel entry : logs) {

                int i = 1;

                ps.setString(i++, entry.getUserId());
                ps.setString(i++, entry.getUrl());

                ps.setString(i++, entry.getInTime() != null ? entry.getInTime().toLocalDateTime().format(formatter)
                        : LocalDateTime.now().format(formatter));

                ps.setString(i++, entry.getOutTime() != null ? entry.getOutTime().toLocalDateTime().format(formatter)
                        : LocalDateTime.now().format(formatter));

                long total = 0;
                ps.setLong(i++, total);

                ps.setString(i++, entry.getModule());
                ps.setString(i++, entry.getMethod());

                String req = MaskingUtil.maskJson(entry.getReqBody());
                ps.setString(i++, req);

                String res = MaskingUtil.maskJson(entry.getResBody());
                if (res != null && res.length() > 8192) {
                    byte[] compressed = commonUtils.compress(res);
                    res = Base64.getEncoder().encodeToString(compressed);
                }

                ps.setString(i++, res);
                ps.setString(i++, "ONT");

                ps.setString(i++, LocalDateTime.now().format(formatter));

                ps.addBatch();
            }

            ps.executeBatch();

        } catch (Exception e) {
            log.error("insertRestAccessLogBatch failed for table {}", tableName, e);
        }
    }

    public void insertAccessLogBatch(String key, List<AccessLogModel> batchLogs) {

        if (batchLogs == null || batchLogs.isEmpty()) {
            return;
        }

        String tableName = batchLogs.get(0).getTableName();

        if (tableName == null) {
            log.info("Table name is null, skipping batch");
            return;
        }

        String insertQuery = "INSERT INTO " + tableName + " "
                + "(user_id, ucc, uri, module, method, source, "
                + "req_body, res_body, device_ip, req_id, "
                + "user_agent, domain, content_type, session, "
                + "in_time, out_time, lag_time, "
                + "vendor, created_on) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection connection = ds.getConnection();
                PreparedStatement statement = connection.prepareStatement(insertQuery)) {

            for (AccessLogModel accLogModel : batchLogs) {

                int i = 1;

                statement.setString(i++, normalize(accLogModel.getUserId()));
                statement.setString(i++, normalize(accLogModel.getUcc()));
                statement.setString(i++, normalize(accLogModel.getUri()));
                statement.setString(i++, normalize(accLogModel.getModule()));
                statement.setString(i++, normalize(accLogModel.getMethod()));
                statement.setString(i++, normalize(accLogModel.getSource()));

                // request body
                statement.setString(i++, normalize(accLogModel.getReqBody()));

                // response body — GZIP+Base64 for payloads over 8 KB
                String respBody = "";
                String res      = accLogModel.getResBody();

                if (StringUtil.isNotNullOrEmpty(res)) {
                    if (res.length() > 8192) {
                        byte[] compressed = commonUtils.compress(res);
                        respBody = Base64.getEncoder().encodeToString(compressed);
                    } else {
                        respBody = res;
                    }
                }

                statement.setString(i++, respBody);

                statement.setString(i++, normalize(accLogModel.getDeviceIp()));
                // statement.setString(i++, normalize(accLogModel.getDeviceId()));
                statement.setString(i++, normalize(accLogModel.getReqId()));
                statement.setString(i++, normalize(accLogModel.getUserAgent()));
                statement.setString(i++, normalize(accLogModel.getDomain()));
                statement.setString(i++, normalize(accLogModel.getContentType()));
                statement.setString(i++, normalize(accLogModel.getSession()));

                statement.setString(i++,
                        accLogModel.getInTime() != null
                                ? accLogModel.getInTime().toLocalDateTime().format(formatter)
                                : LocalDateTime.now().format(formatter));

                statement.setString(i++,
                        accLogModel.getOutTime() != null
                                ? accLogModel.getOutTime().toLocalDateTime().format(formatter)
                                : LocalDateTime.now().format(formatter));

                statement.setLong(i++, Math.max(accLogModel.getLagTime(), 0L));

                statement.setString(i++, normalize(accLogModel.getVendor()));

                // created_on
                statement.setString(i++, LocalDateTime.now().format(formatter));

                statement.addBatch();
            }

            statement.executeBatch();

        } catch (Exception e) {
            log.error("insertAccessLogBatch failed - {}", e.getMessage(), e);
        }
    }

    private String normalize(String val) {
        return (val == null || val.trim().isEmpty()) ? "" : val.trim();
    }
}
