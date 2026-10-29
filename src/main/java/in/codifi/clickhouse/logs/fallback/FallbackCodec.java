package in.codifi.clickhouse.logs.fallback;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;

/**
 * JSON Lines encoding of a failed batch.
 *
 * <pre>
 *   line 1 : {"_fallback":1,"logType":"access","masked":true,"rows":N}
 *   line 2+: one row per line, every model field (timestamps as epoch millis)
 * </pre>
 *
 * json-simple escapes control characters, so a body containing newlines stays on one line.
 */
final class FallbackCodec {

    static final int FORMAT_VERSION = 1;

    private FallbackCodec() {
    }

    static final class Decoded {
        final LogType type;
        final boolean masked;
        final List<?> rows;

        Decoded(LogType type, boolean masked, List<?> rows) {
            this.type   = type;
            this.masked = masked;
            this.rows   = rows;
        }
    }

    // ---- encode ----

    /** {@code bodyMapper} is applied to req/res bodies (masking, or identity). */
    @SuppressWarnings("unchecked")
    static byte[] encode(LogType type, List<?> rows, boolean masked, UnaryOperator<String> bodyMapper) {
        StringBuilder sb = new StringBuilder();
        JSONObject header = new JSONObject();
        header.put("_fallback", FORMAT_VERSION);
        header.put("logType", type.dir());
        header.put("masked", masked);
        header.put("rows", rows.size());
        sb.append(header.toJSONString()).append('\n');

        for (Object row : rows) {
            JSONObject o;
            switch (type) {
                case ACCESS: o = encodeAccess((AccessLogModel) row, bodyMapper); break;
                case REST:   o = encodeRest((RestAccessLogModel) row, bodyMapper); break;
                default:     o = encodeLogin((UserLoginReportModel) row);          break;
            }
            sb.append(o.toJSONString()).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** One access/rest row as a single-line JSON string, same fields as a fallback file row. */
    static String encodeRow(LogType type, Object row, UnaryOperator<String> bodyMapper) {
        switch (type) {
            case ACCESS: return encodeAccess((AccessLogModel) row, bodyMapper).toJSONString();
            case REST:   return encodeRest((RestAccessLogModel) row, bodyMapper).toJSONString();
            default:     throw new IllegalArgumentException("encodeRow supports access/rest only");
        }
    }

    @SuppressWarnings("unchecked")
    private static JSONObject encodeAccess(AccessLogModel m, UnaryOperator<String> bodyMapper) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("ucc", m.getUcc());
        o.put("uri", m.getUri());
        o.put("module", m.getModule());
        o.put("method", m.getMethod());
        o.put("statusCode", m.getStatusCode());
        o.put("source", m.getSource());
        o.put("reqBody", bodyMapper.apply(m.getReqBody()));
        o.put("resBody", bodyMapper.apply(m.getResBody()));
        o.put("deviceIp", m.getDeviceIp());
        o.put("deviceId", m.getDeviceId());
        o.put("reqId", m.getReqId());
        o.put("userAgent", m.getUserAgent());
        o.put("domain", m.getDomain());
        o.put("contentType", m.getContentType());
        o.put("session", m.getSession());
        o.put("inTime", millis(m.getInTime()));
        o.put("outTime", millis(m.getOutTime()));
        o.put("vendor", m.getVendor());
        o.put("filler1", m.getFiller1());
        o.put("filler2", m.getFiller2());
        o.put("podId", m.getPodId());
        return o;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject encodeRest(RestAccessLogModel m, UnaryOperator<String> bodyMapper) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("ucc", m.getUcc());
        o.put("module", m.getModule());
        o.put("deviceId", m.getDeviceId());
        o.put("method", m.getMethod());
        o.put("statusCode", m.getStatusCode());
        o.put("url", m.getUrl());
        o.put("reqId", m.getReqId());
        o.put("reqBody", bodyMapper.apply(m.getReqBody()));
        o.put("resBody", bodyMapper.apply(m.getResBody()));
        o.put("inTime", millis(m.getInTime()));
        o.put("outTime", millis(m.getOutTime()));
        o.put("totalTime", m.getTotalTime());
        o.put("vendor", m.getVendor());
        o.put("source", m.getSource());
        o.put("filler1", m.getFiller1());
        o.put("filler2", m.getFiller2());
        o.put("podId", m.getPodId());
        return o;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject encodeLogin(UserLoginReportModel m) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("source", m.getSource());
        o.put("vendor", m.getVendor());
        o.put("visitors", m.getVisitors());
        return o;
    }

    // ---- decode ----

    /** Throws on any malformed content; the caller quarantines the file. */
    static Decoded decode(byte[] data) throws Exception {
        JSONParser parser = new JSONParser();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8))) {

            String first = r.readLine();
            if (first == null) throw new IllegalStateException("empty fallback file");
            JSONObject header = (JSONObject) parser.parse(first);
            LogType type = LogType.fromDir(str(header, "logType"));
            if (type == null || header.get("_fallback") == null) {
                throw new IllegalStateException("missing/invalid fallback header");
            }
            boolean masked = Boolean.TRUE.equals(header.get("masked"));
            Long expected  = (Long) header.get("rows");

            List<Object> rows = new ArrayList<>();
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isEmpty()) continue;
                JSONObject o = (JSONObject) parser.parse(line);
                switch (type) {
                    case ACCESS: rows.add(decodeAccess(o)); break;
                    case REST:   rows.add(decodeRest(o));   break;
                    default:     rows.add(decodeLogin(o));  break;
                }
            }
            if (expected != null && expected != rows.size()) {
                throw new IllegalStateException("row count mismatch: header " + expected + ", found " + rows.size());
            }
            return new Decoded(type, masked, rows);
        }
    }

    private static AccessLogModel decodeAccess(JSONObject o) {
        AccessLogModel m = new AccessLogModel();
        m.setUserId(str(o, "userId"));
        m.setUcc(str(o, "ucc"));
        m.setUri(str(o, "uri"));
        m.setModule(str(o, "module"));
        m.setMethod(str(o, "method"));
        m.setStatusCode(integer(o, "statusCode"));
        m.setSource(str(o, "source"));
        m.setReqBody(str(o, "reqBody"));
        m.setResBody(str(o, "resBody"));
        m.setDeviceIp(str(o, "deviceIp"));
        m.setDeviceId(str(o, "deviceId"));
        m.setReqId(str(o, "reqId"));
        m.setUserAgent(str(o, "userAgent"));
        m.setDomain(str(o, "domain"));
        m.setContentType(str(o, "contentType"));
        m.setSession(str(o, "session"));
        m.setInTime(timestamp(o, "inTime"));
        m.setOutTime(timestamp(o, "outTime"));
        m.setVendor(str(o, "vendor"));
        m.setFiller1(str(o, "filler1"));
        m.setFiller2(str(o, "filler2"));
        m.setPodId(str(o, "podId"));
        return m;
    }

    private static RestAccessLogModel decodeRest(JSONObject o) {
        RestAccessLogModel m = new RestAccessLogModel();
        m.setUserId(str(o, "userId"));
        m.setUcc(str(o, "ucc"));
        m.setModule(str(o, "module"));
        m.setDeviceId(str(o, "deviceId"));
        m.setMethod(str(o, "method"));
        m.setStatusCode(integer(o, "statusCode"));
        m.setUrl(str(o, "url"));
        m.setReqId(str(o, "reqId"));
        m.setReqBody(str(o, "reqBody"));
        m.setResBody(str(o, "resBody"));
        m.setInTime(timestamp(o, "inTime"));
        m.setOutTime(timestamp(o, "outTime"));
        m.setTotalTime(lng(o, "totalTime"));
        m.setVendor(str(o, "vendor"));
        m.setSource(str(o, "source"));
        m.setFiller1(str(o, "filler1"));
        m.setFiller2(str(o, "filler2"));
        m.setPodId(str(o, "podId"));
        return m;
    }

    private static UserLoginReportModel decodeLogin(JSONObject o) {
        UserLoginReportModel m = new UserLoginReportModel();
        m.setUserId(str(o, "userId"));
        m.setSource(str(o, "source"));
        m.setVendor(str(o, "vendor"));
        m.setVisitors(integer(o, "visitors"));
        return m;
    }

    // ---- field helpers ----

    private static Long millis(Timestamp t) {
        return t != null ? t.getTime() : null;
    }

    private static String str(JSONObject o, String key) {
        Object v = o.get(key);
        return v != null ? v.toString() : null;
    }

    private static Long lng(JSONObject o, String key) {
        Object v = o.get(key);
        return v instanceof Number ? ((Number) v).longValue() : null;
    }

    private static Integer integer(JSONObject o, String key) {
        Object v = o.get(key);
        return v instanceof Number ? ((Number) v).intValue() : null;
    }

    private static Timestamp timestamp(JSONObject o, String key) {
        Long v = lng(o, key);
        return v != null ? new Timestamp(v) : null;
    }
}
