package in.codifi.clickhouse.logs.fallback;

/** Log streams that can be routed to the fallback store. {@link #dir()} is the top-level folder name. */
public enum LogType {

    ACCESS("access"),
    REST("rest_access"),
    LOGIN("login"),
    EMAIL("email"),
    SMS("sms"),
    ORDER("order");

    private final String dir;

    LogType(String dir) {
        this.dir = dir;
    }

    public String dir() {
        return dir;
    }

    /** Accepts the config spellings: access | rest | rest_access | login | email | sms | order (case-insensitive). */
    static LogType fromConfig(String value) {
        if (value == null) return null;
        switch (value.trim().toLowerCase()) {
            case "access":      return ACCESS;
            case "rest":
            case "rest_access": return REST;
            case "login":       return LOGIN;
            case "email":       return EMAIL;
            case "sms":         return SMS;
            case "order":       return ORDER;
            default:            return null;
        }
    }

    static LogType fromDir(String dir) {
        for (LogType t : values()) {
            if (t.dir.equals(dir)) return t;
        }
        return null;
    }
}
