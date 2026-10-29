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
import in.codifi.clickhouse.logs.model.EmailLogModel;
import in.codifi.clickhouse.logs.model.OrderDetailModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.SmsLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;
import in.codifi.clickhouse.logs.util.MaskingUtil;

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
                case EMAIL:  o = encodeEmail((EmailLogModel) row);                 break;
                case SMS:    o = encodeSms((SmsLogModel) row);                     break;
                case ORDER:  o = encodeOrder((OrderDetailModel) row);              break;
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

    @SuppressWarnings("unchecked")
    private static JSONObject encodeEmail(EmailLogModel m) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("email", m.getEmail());
        o.put("otp", MaskingUtil.maskOtp(m.getOtp())); // never write the real OTP to a fallback file
        o.put("emailHost", m.getEmailHost());
        o.put("emailPort", m.getEmailPort());
        o.put("emailFrom", m.getEmailFrom());
        o.put("emailSubject", m.getEmailSubject());
        o.put("emailContent", m.getEmailContent());
        o.put("emailResponse", m.getEmailResponse());
        o.put("emailStatus", m.getEmailStatus());
        o.put("createdBy", m.getCreatedBy());
        o.put("source", m.getSource());
        o.put("filler", m.getFiller());
        return o;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject encodeSms(SmsLogModel m) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("mobile", m.getMobile());
        o.put("otp", MaskingUtil.maskOtp(m.getOtp())); // never write the real OTP to a fallback file
        o.put("url", m.getUrl());
        o.put("requestMessage", m.getRequestMessage());
        o.put("responseMessage", m.getResponseMessage());
        o.put("sentStatus", m.getSentStatus());
        o.put("createdBy", m.getCreatedBy());
        o.put("source", m.getSource());
        o.put("filler", m.getFiller());
        return o;
    }

    @SuppressWarnings("unchecked")
    private static JSONObject encodeOrder(OrderDetailModel m) {
        JSONObject o = new JSONObject();
        o.put("userId", m.getUserId());
        o.put("accId", m.getAccId());
        o.put("user", m.getUser());
        o.put("token", m.getToken());
        o.put("exchange", m.getExchange());
        o.put("tradingSymbol", m.getTradingSymbol());
        o.put("action", m.getAction());
        o.put("transType", m.getTransType());
        o.put("product", m.getProduct());
        o.put("validity", m.getValidity());
        o.put("prcType", m.getPrcType());
        o.put("orderType", m.getOrderType());
        o.put("customerFirm", m.getCustomerFirm());
        o.put("qty", m.getQty());
        o.put("price", m.getPrice());
        o.put("triggerPrice", m.getTriggerPrice());
        o.put("discQty", m.getDiscQty());
        o.put("cancelledSize", m.getCancelledSize());
        o.put("mktProtectionPrice", m.getMktProtectionPrice());
        o.put("orderNo", m.getOrderNo());
        o.put("target", m.getTarget());
        o.put("stopLossValue", m.getStopLossValue());
        o.put("trailingStopLoss", m.getTrailingStopLoss());
        o.put("tickSize", m.getTickSize());
        o.put("ltpOrAtp", m.getLtpOrAtp());
        o.put("sqrOffAbsOrTick", m.getSqrOffAbsOrTick());
        o.put("slabsOrTick", m.getSlabsOrTick());
        o.put("trailTickYesNo", m.getTrailTickYesNo());
        o.put("classification", m.getClassification());
        o.put("vendorCode", m.getVendorCode());
        o.put("flagToAddCategory", m.getFlagToAddCategory());
        o.put("orderCriteria", m.getOrderCriteria());
        o.put("posSqrOffFlag", m.getPosSqrOffFlag());
        o.put("algoId", m.getAlgoId());
        o.put("orderTag", m.getOrderTag());
        o.put("comments", m.getComments());
        o.put("source", m.getSource());
        o.put("method", m.getMethod());
        o.put("module", m.getModule());
        o.put("deviceIp", m.getDeviceIp());
        o.put("deviceId", m.getDeviceId());
        o.put("status", m.getStatus());
        o.put("rejReason", m.getRejReason());
        o.put("createdBy", m.getCreatedBy());
        o.put("activeStatus", m.getActiveStatus());
        o.put("appCode", m.getAppCode());
        o.put("sourcePlatform", m.getSourcePlatform());
        o.put("filler1", m.getFiller1());
        o.put("filler2", m.getFiller2());
        o.put("filler3", m.getFiller3());
        o.put("filler4", m.getFiller4());
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
                    case EMAIL:  rows.add(decodeEmail(o));  break;
                    case SMS:    rows.add(decodeSms(o));    break;
                    case ORDER:  rows.add(decodeOrder(o));  break;
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

    private static EmailLogModel decodeEmail(JSONObject o) {
        EmailLogModel m = new EmailLogModel();
        m.setUserId(str(o, "userId"));
        m.setEmail(str(o, "email"));
        m.setOtp(str(o, "otp"));
        m.setEmailHost(str(o, "emailHost"));
        m.setEmailPort(str(o, "emailPort"));
        m.setEmailFrom(str(o, "emailFrom"));
        m.setEmailSubject(str(o, "emailSubject"));
        m.setEmailContent(str(o, "emailContent"));
        m.setEmailResponse(str(o, "emailResponse"));
        m.setEmailStatus(str(o, "emailStatus"));
        m.setCreatedBy(str(o, "createdBy"));
        m.setSource(str(o, "source"));
        m.setFiller(str(o, "filler"));
        return m;
    }

    private static SmsLogModel decodeSms(JSONObject o) {
        SmsLogModel m = new SmsLogModel();
        m.setUserId(str(o, "userId"));
        m.setMobile(str(o, "mobile"));
        m.setOtp(str(o, "otp"));
        m.setUrl(str(o, "url"));
        m.setRequestMessage(str(o, "requestMessage"));
        m.setResponseMessage(str(o, "responseMessage"));
        m.setSentStatus(str(o, "sentStatus"));
        m.setCreatedBy(str(o, "createdBy"));
        m.setSource(str(o, "source"));
        m.setFiller(str(o, "filler"));
        return m;
    }

    private static OrderDetailModel decodeOrder(JSONObject o) {
        OrderDetailModel m = new OrderDetailModel();
        m.setUserId(str(o, "userId"));
        m.setAccId(str(o, "accId"));
        m.setUser(str(o, "user"));
        m.setToken(str(o, "token"));
        m.setExchange(str(o, "exchange"));
        m.setTradingSymbol(str(o, "tradingSymbol"));
        m.setAction(str(o, "action"));
        m.setTransType(str(o, "transType"));
        m.setProduct(str(o, "product"));
        m.setValidity(str(o, "validity"));
        m.setPrcType(str(o, "prcType"));
        m.setOrderType(str(o, "orderType"));
        m.setCustomerFirm(str(o, "customerFirm"));
        m.setQty(str(o, "qty"));
        m.setPrice(str(o, "price"));
        m.setTriggerPrice(str(o, "triggerPrice"));
        m.setDiscQty(str(o, "discQty"));
        m.setCancelledSize(str(o, "cancelledSize"));
        m.setMktProtectionPrice(str(o, "mktProtectionPrice"));
        m.setOrderNo(str(o, "orderNo"));
        m.setTarget(str(o, "target"));
        m.setStopLossValue(str(o, "stopLossValue"));
        m.setTrailingStopLoss(str(o, "trailingStopLoss"));
        m.setTickSize(str(o, "tickSize"));
        m.setLtpOrAtp(str(o, "ltpOrAtp"));
        m.setSqrOffAbsOrTick(str(o, "sqrOffAbsOrTick"));
        m.setSlabsOrTick(str(o, "slabsOrTick"));
        m.setTrailTickYesNo(str(o, "trailTickYesNo"));
        m.setClassification(str(o, "classification"));
        m.setVendorCode(str(o, "vendorCode"));
        m.setFlagToAddCategory(str(o, "flagToAddCategory"));
        m.setOrderCriteria(str(o, "orderCriteria"));
        m.setPosSqrOffFlag(str(o, "posSqrOffFlag"));
        m.setAlgoId(str(o, "algoId"));
        m.setOrderTag(str(o, "orderTag"));
        m.setComments(str(o, "comments"));
        m.setSource(str(o, "source"));
        m.setMethod(str(o, "method"));
        m.setModule(str(o, "module"));
        m.setDeviceIp(str(o, "deviceIp"));
        m.setDeviceId(str(o, "deviceId"));
        m.setStatus(str(o, "status"));
        m.setRejReason(str(o, "rejReason"));
        m.setCreatedBy(str(o, "createdBy"));
        m.setActiveStatus(str(o, "activeStatus"));
        m.setAppCode(str(o, "appCode"));
        m.setSourcePlatform(str(o, "sourcePlatform"));
        m.setFiller1(str(o, "filler1"));
        m.setFiller2(str(o, "filler2"));
        m.setFiller3(str(o, "filler3"));
        m.setFiller4(str(o, "filler4"));
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
