package in.codifi.clickhouse.logs.util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class MaskingUtil {

    private static final String PAN_MASK    = "M:3:1";
    private static final String AADHAR_MASK = "M:0:4";
    private static final String MOBILE_MASK = "M:2:2";
    private static final String EMAIL_MASK  = "M:2:3";

    public static String maskJson(String jsonStr) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return jsonStr;
        }
        try {
            JSONParser parser = new JSONParser();
            Object parsed = parser.parse(jsonStr);
            Object masked = process(parsed);
            return masked.toString();
        } catch (Exception e) {
            return jsonStr;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object process(Object obj) {
        if (obj instanceof JSONObject) {
            JSONObject json = (JSONObject) obj;
            for (Object keyObj : json.keySet()) {
                String key = keyObj.toString();
                Object value = json.get(key);
                if (value instanceof JSONObject || value instanceof JSONArray) {
                    json.put(key, process(value));
                } else if (value != null) {
                    String masked = maskValue(key, value.toString());
                    json.put(key, masked);
                }
            }
            return json;
        } else if (obj instanceof JSONArray) {
            JSONArray array = (JSONArray) obj;
            for (int i = 0; i < array.size(); i++) {
                array.set(i, process(array.get(i)));
            }
            return array;
        }
        return obj;
    }

    private static String maskValue(String key, String value) {
        String config = getMaskConfig(key);
        if (config == null)
            return value;
        String[] parts = config.split(":");
        int first = Integer.parseInt(parts[1]);
        int last  = Integer.parseInt(parts[2]);
        return applyMask(value, first, last);
    }

    private static String getMaskConfig(String key) {
        switch (key.toLowerCase()) {
            case "pan_no":  return PAN_MASK;
            case "aadhar":  return AADHAR_MASK;
            case "mobile":  return MOBILE_MASK;
            case "email":   return EMAIL_MASK;
            default:        return null;
        }
    }

    private static String applyMask(String value, int first, int last) {
        if (value == null)
            return null;
        int len = value.length();
        if (len <= (first + last))
            return value;
        String start = value.substring(0, first);
        String end   = value.substring(len - last);
        StringBuilder stars = new StringBuilder();
        for (int i = 0; i < len - (first + last); i++) {
            stars.append("*");
        }
        return start + stars + end;
    }

    private static final String OTP_MASK = "****";

    /** Replaces any OTP with a fixed mask; null/empty is returned as-is. Idempotent. */
    public static String maskOtp(String otp) {
        return otp == null || otp.isEmpty() ? otp : OTP_MASK;
    }
}
