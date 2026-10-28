package in.codifi.clickhouse.logs.fallback;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fallback-store settings. Disabled by default.
 *
 * <p>Build it with the fluent setters, or let the producer/consumer hand over its own
 * configuration via {@link #fromProperties(Properties)} / {@link #fromMap(Map)}, which read the
 * {@code clickhouse.fallback.*} keys. Invalid values are logged as WARN and replaced by their
 * default — parsing never throws.
 */
public class FallbackConfig {

    private static final Logger log = LoggerFactory.getLogger(FallbackConfig.class);

    public static final String PREFIX = "clickhouse.fallback.";

    public static final String TYPE_DISK = "disk";
    public static final String TYPE_S3   = "s3";

    private boolean      enabled                 = false;
    private String       type;
    private Long         maxSizeMb;
    private int          recoveryIntervalSeconds = 300;
    private Set<LogType> logTypes                = EnumSet.of(LogType.ACCESS, LogType.REST);
    private String       instanceName;

    private String diskBasePath;

    private String s3Endpoint;
    private String s3Bucket;
    private String s3BasePath;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3Region;

    // ---- factories ----

    public static FallbackConfig disabled() {
        return new FallbackConfig();
    }

    public static FallbackConfig fromProperties(Properties props) {
        if (props == null) return disabled();
        return parse(props::getProperty);
    }

    public static FallbackConfig fromMap(Map<String, ?> map) {
        if (map == null) return disabled();
        return parse(k -> {
            Object v = map.get(k);
            return v != null ? v.toString() : null;
        });
    }

    private static FallbackConfig parse(Function<String, String> source) {
        FallbackConfig c = new FallbackConfig();
        try {
            c.enabled                 = parseBoolean(source, "enabled", false);
            c.type                    = trimToNull(source.apply(PREFIX + "type"));
            c.maxSizeMb               = parseLong(source, "max.size.mb");
            Long interval             = parseLong(source, "recovery.interval.seconds");
            c.recoveryIntervalSeconds = interval != null ? interval.intValue() : 300;
            String types              = trimToNull(source.apply(PREFIX + "log.types"));
            if (types != null) c.setLogTypes(types);
            c.instanceName            = trimToNull(source.apply(PREFIX + "instance.name"));
            c.diskBasePath            = trimToNull(source.apply(PREFIX + "disk.base.path"));
            c.s3Endpoint              = trimToNull(source.apply(PREFIX + "s3.endpoint"));
            c.s3Bucket                = trimToNull(source.apply(PREFIX + "s3.bucket"));
            c.s3BasePath              = trimToNull(source.apply(PREFIX + "s3.base.path"));
            c.s3AccessKey             = trimToNull(source.apply(PREFIX + "s3.access.key"));
            c.s3SecretKey             = trimToNull(source.apply(PREFIX + "s3.secret.key"));
            c.s3Region                = trimToNull(source.apply(PREFIX + "s3.region"));
        } catch (Exception e) {
            log.warn("Could not read fallback config ({}); fallback disabled", e.getMessage());
            return disabled();
        }
        return c;
    }

    // ---- fluent setters ----

    public FallbackConfig setEnabled(boolean enabled)            { this.enabled = enabled; return this; }
    public FallbackConfig setType(String type)                   { this.type = trimToNull(type); return this; }
    public FallbackConfig setMaxSizeMb(Long maxSizeMb)           { this.maxSizeMb = maxSizeMb; return this; }
    public FallbackConfig setInstanceName(String instanceName)   { this.instanceName = trimToNull(instanceName); return this; }
    public FallbackConfig setDiskBasePath(String diskBasePath)   { this.diskBasePath = trimToNull(diskBasePath); return this; }
    public FallbackConfig setS3Endpoint(String s3Endpoint)       { this.s3Endpoint = trimToNull(s3Endpoint); return this; }
    public FallbackConfig setS3Bucket(String s3Bucket)           { this.s3Bucket = trimToNull(s3Bucket); return this; }
    public FallbackConfig setS3BasePath(String s3BasePath)       { this.s3BasePath = trimToNull(s3BasePath); return this; }
    public FallbackConfig setS3AccessKey(String s3AccessKey)     { this.s3AccessKey = trimToNull(s3AccessKey); return this; }
    public FallbackConfig setS3SecretKey(String s3SecretKey)     { this.s3SecretKey = trimToNull(s3SecretKey); return this; }
    public FallbackConfig setS3Region(String s3Region)           { this.s3Region = trimToNull(s3Region); return this; }

    public FallbackConfig setRecoveryIntervalSeconds(int seconds) {
        this.recoveryIntervalSeconds = seconds;
        return this;
    }

    /** Comma-separated list, e.g. {@code "access,rest,login"}. Unknown entries are WARNed and skipped. */
    public FallbackConfig setLogTypes(String csv) {
        Set<LogType> parsed = EnumSet.noneOf(LogType.class);
        if (csv != null) {
            for (String part : csv.split(",")) {
                if (part.trim().isEmpty()) continue;
                LogType t = LogType.fromConfig(part);
                if (t != null) parsed.add(t);
                else log.warn("Unknown {}log.types entry '{}' ignored", PREFIX, part.trim());
            }
        }
        this.logTypes = parsed;
        return this;
    }

    // ---- getters ----

    public boolean      isEnabled()                  { return enabled; }
    public String       getType()                    { return type; }
    public Long         getMaxSizeMb()               { return maxSizeMb; }
    public int          getRecoveryIntervalSeconds() { return recoveryIntervalSeconds; }
    public Set<LogType> getLogTypes()                { return Collections.unmodifiableSet(logTypes); }
    public String       getInstanceName()            { return instanceName; }
    public String       getDiskBasePath()            { return diskBasePath; }
    public String       getS3Endpoint()              { return s3Endpoint; }
    public String       getS3Bucket()                { return s3Bucket; }
    public String       getS3BasePath()              { return s3BasePath; }
    public String       getS3AccessKey()             { return s3AccessKey; }
    public String       getS3SecretKey()             { return s3SecretKey; }
    public String       getS3Region()                { return s3Region; }

    /** Secrets are never included. */
    @Override
    public String toString() {
        return "FallbackConfig{enabled=" + enabled + ", type=" + type + ", maxSizeMb=" + maxSizeMb
                + ", recoveryIntervalSeconds=" + recoveryIntervalSeconds + ", logTypes=" + logTypes
                + ", instanceName=" + instanceName + ", diskBasePath=" + diskBasePath
                + ", s3Endpoint=" + s3Endpoint + ", s3Bucket=" + s3Bucket + ", s3BasePath=" + s3BasePath
                + ", s3Region=" + s3Region + "}";
    }

    // ---- parsing helpers ----

    private static boolean parseBoolean(Function<String, String> source, String key, boolean def) {
        String v = trimToNull(source.apply(PREFIX + key));
        if (v == null) return def;
        if (v.equalsIgnoreCase("true"))  return true;
        if (v.equalsIgnoreCase("false")) return false;
        log.warn("Invalid {}{}='{}'; using {}", PREFIX, key, v, def);
        return def;
    }

    private static Long parseLong(Function<String, String> source, String key) {
        String v = trimToNull(source.apply(PREFIX + key));
        if (v == null) return null;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            log.warn("Invalid {}{}='{}'; ignoring", PREFIX, key, v);
            return null;
        }
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
