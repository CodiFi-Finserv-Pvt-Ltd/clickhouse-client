package in.codifi.clickhouse.logs.fallback;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.AccessLogWriter;
import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;
import in.codifi.clickhouse.logs.util.MaskingUtil;

/**
 * Routes failed batches to a {@link FallbackStore} and replays them into ClickHouse.
 *
 * <p>Layout: {@code <base>/<log-type>/<module>/<instance>_<yyyyMMddHHmmssSSS>_<counter>.jsonl},
 * one new file per (failed batch, module). A file is deleted only after its insert succeeded.
 *
 * <p>Nothing here throws to the caller: {@link #offer} returns the rows it could NOT store, and the
 * caller keeps those in the existing in-memory retry buffer.
 */
public final class FallbackManager {

    private static final Logger log = LoggerFactory.getLogger(FallbackManager.class);

    /** Per-row outage stream (access/rest only), so it can be routed or filtered on its own. */
    private static final Logger outageLog = LoggerFactory.getLogger("in.codifi.clickhouse.logs.outage");

    private static final String            SUFFIX          = ".jsonl";
    private static final String            UNKNOWN         = "unknown";
    private static final int               FIRST_CYCLE_SEC = 10;
    private static final DateTimeFormatter TS              = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    private final FallbackStore   store;
    private final DataSource      ds;
    private final AccessLogWriter writer;    // masks bodies when maskingEnabled
    private final AccessLogWriter rawWriter; // never masks — for files whose bodies are already masked
    private final boolean         maskingEnabled;
    private final boolean         outageLogInfo;
    private final Set<LogType>    logTypes;
    private final long            maxBytes;
    private final int             intervalSeconds;

    private final String  instanceName; // host / pod name or the configured override
    private final String  instanceId;   // instanceName, plus "-<pid>" when derived
    private final long    pid;
    private final boolean canAdopt;

    private final String restTable;
    private final String accessTable;
    private final String loginTable;

    private final AtomicLong    sizeBytes = new AtomicLong(0L);
    private final AtomicLong    counter   = new AtomicLong(0L);
    private final AtomicBoolean overLimit = new AtomicBoolean(false);

    private final ScheduledExecutorService recovery;

    private FallbackManager(FallbackConfig cfg, FallbackStore store, DataSource ds, String database,
            boolean maskingEnabled) {
        this.store          = store;
        this.ds             = ds;
        this.writer         = new AccessLogWriter(ds, maskingEnabled);
        this.rawWriter      = new AccessLogWriter(ds, false);
        this.maskingEnabled = maskingEnabled;
        this.outageLogInfo  = cfg.isOutageLogInfo();
        this.logTypes       = cfg.getLogTypes();
        this.maxBytes       = cfg.getMaxSizeMb() * 1024L * 1024L;

        int interval = cfg.getRecoveryIntervalSeconds();
        if (interval < 1) {
            log.warn("{}recovery.interval.seconds ({}) is invalid; defaulting to 300", FallbackConfig.PREFIX, interval);
            interval = 300;
        }
        this.intervalSeconds = interval;

        // Same names AccessLogBatchService inserts into.
        this.restTable   = database + ".tbl_rest_access_log";
        this.accessTable = database + ".tbl_access_log";
        this.loginTable  = database + ".tbl_user_loggedin_report";

        this.pid = currentPid();
        if (cfg.getInstanceName() != null) {
            // Explicit override: used as-is so it survives restarts (e.g. StatefulSet pod names).
            this.instanceName = sanitize(cfg.getInstanceName());
            this.instanceId   = instanceName;
            this.canAdopt     = false;
        } else {
            this.instanceName = sanitize(hostName());
            this.instanceId   = instanceName + "-" + (pid > 0 ? Long.toString(pid) : shortRandom());
            this.canAdopt     = pid > 0 && store.supportsAtomicRename();
        }

        this.recovery = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "clickhouse-fallback-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Builds and starts a manager, or returns {@code null} (with a log line) when fallback is
     * disabled or not usable — the caller then uses the in-memory retry buffer. Never throws.
     */
    public static FallbackManager create(FallbackConfig cfg, DataSource ds, String database, boolean maskingEnabled) {
        try {
            if (cfg == null || !cfg.isEnabled()) {
                return null;
            }
            if (cfg.getLogTypes().isEmpty()) {
                log.warn("ClickHouse fallback enabled but {}log.types is empty; using in-memory retry buffer",
                        FallbackConfig.PREFIX);
                return null;
            }
            if (cfg.getMaxSizeMb() == null || cfg.getMaxSizeMb() <= 0) {
                log.warn("ClickHouse fallback enabled but {}max.size.mb is not set (> 0); using in-memory retry buffer",
                        FallbackConfig.PREFIX);
                return null;
            }

            FallbackStore store;
            String type = cfg.getType() != null ? cfg.getType().toLowerCase() : null;
            if (FallbackConfig.TYPE_DISK.equals(type)) {
                if (cfg.getDiskBasePath() == null) {
                    log.warn("ClickHouse fallback type=disk but {}disk.base.path is not set; using in-memory retry buffer",
                            FallbackConfig.PREFIX);
                    return null;
                }
                store = new DiskFallbackStore(cfg.getDiskBasePath());
            } else if (FallbackConfig.TYPE_S3.equals(type)) {
                store = new S3FallbackStore(cfg);
            } else {
                log.warn("ClickHouse fallback type '{}' is not disk|s3; using in-memory retry buffer", cfg.getType());
                return null;
            }

            FallbackManager m = new FallbackManager(cfg, store, ds, database, maskingEnabled);
            m.start();
            log.info("ClickHouse fallback enabled: store={}, instance={}, maxSizeMb={}, logTypes={}, recoveryIntervalSeconds={}",
                    store.describe(), m.instanceId, cfg.getMaxSizeMb(), cfg.getLogTypes(), m.intervalSeconds);
            return m;
        } catch (Throwable t) {
            log.warn("ClickHouse fallback could not be initialised ({}); using in-memory retry buffer", t.toString());
            return null;
        }
    }

    private void start() {
        recovery.scheduleWithFixedDelay(this::recoverOnce, FIRST_CYCLE_SEC, intervalSeconds, TimeUnit.SECONDS);
    }

    public void shutdown() {
        recovery.shutdown();
    }

    public String getInstanceId() {
        return instanceId;
    }

    // ---- write path ----

    /**
     * When {@code clickhouse.fallback.outage.loginfo} is on, logs every row of a failed ACCESS/REST
     * batch at INFO (one JSON line each, bodies masked like the fallback file) so the rows reach the
     * log pipeline even if the pod dies before they are replayed. LOGIN is never logged. Never throws.
     */
    public void logOutageRows(LogType type, List<?> rows) {
        if (!outageLogInfo || rows == null || (type != LogType.ACCESS && type != LogType.REST)) return;
        UnaryOperator<String> bodyMapper = maskingEnabled ? MaskingUtil::maskJson : UnaryOperator.identity();
        for (Object row : rows) {
            try {
                outageLog.info("ClickHouse outage {} log: {}", type.dir(), FallbackCodec.encodeRow(type, row, bodyMapper));
            } catch (Throwable t) {
                log.warn("Could not write outage INFO for a {} row ({})", type.dir(), t.getClass().getSimpleName());
            }
        }
    }

    /**
     * Stores the batch, one file per module. Returns the rows that could NOT be stored (empty when
     * everything was written); those belong in the in-memory retry buffer. Never throws.
     */
    public <T> List<T> offer(LogType type, List<T> rows) {
        if (rows == null || rows.isEmpty() || !logTypes.contains(type)) return rows;

        Map<String, List<T>> byModule = new LinkedHashMap<>();
        try {
            for (T row : rows) {
                byModule.computeIfAbsent(moduleOf(type, row), k -> new ArrayList<>()).add(row);
            }
        } catch (Throwable t) {
            log.warn("Fallback grouping failed ({}); {} {} rows kept in in-memory retry buffer",
                    t.toString(), rows.size(), type.dir());
            return rows;
        }

        List<T> leftover = new ArrayList<>();
        for (Map.Entry<String, List<T>> e : byModule.entrySet()) {
            if (!writeFile(type, e.getKey(), e.getValue())) leftover.addAll(e.getValue());
        }
        return leftover;
    }

    private boolean writeFile(LogType type, String module, List<?> rows) {
        long reserved = 0L;
        try {
            boolean masked = maskingEnabled && type != LogType.LOGIN;
            UnaryOperator<String> bodyMapper = masked ? MaskingUtil::maskJson : UnaryOperator.identity();
            byte[] data = FallbackCodec.encode(type, rows, masked, bodyMapper);

            if (sizeBytes.addAndGet(data.length) > maxBytes) {
                sizeBytes.addAndGet(-data.length);
                if (overLimit.compareAndSet(false, true)) {
                    log.warn("Fallback store over max size ({} bytes) — using in-memory retry buffer until it drains",
                            maxBytes);
                }
                return false;
            }
            reserved = data.length;

            String path = type.dir() + "/" + module + "/" + newFileName();
            store.write(path, data);

            if (overLimit.compareAndSet(true, false)) {
                log.info("Fallback store back under max size — writing failed batches to {} again", store.describe());
            }
            log.warn("ClickHouse insert failed — {} {} rows saved to fallback {}", rows.size(), type.dir(), path);
            return true;
        } catch (Throwable t) {
            if (reserved > 0) sizeBytes.addAndGet(-reserved);
            log.warn("Fallback write failed ({}); {} {} rows kept in in-memory retry buffer",
                    t.toString(), rows.size(), type.dir());
            return false;
        }
    }

    private String newFileName() {
        String ts = ZonedDateTime.now(ZoneOffset.UTC).format(TS);
        return instanceId + "_" + ts + "_" + String.format("%06d", counter.incrementAndGet()) + SUFFIX;
    }

    // ---- recovery ----

    void recoverOnce() {
        try {
            if (canAdopt) adoptOrphans();

            List<FallbackStore.Entry> own = store.list(instanceId + "_");
            long total = 0L;
            for (FallbackStore.Entry e : own) total += e.size;
            sizeBytes.set(total);
            if (own.isEmpty()) return;

            if (!ping()) {
                log.warn("ClickHouse not reachable; {} fallback file(s) kept for next cycle", own.size());
                return;
            }

            own.sort(Comparator.comparing(FallbackStore.Entry::fileName));
            int files = 0;
            long rows = 0L;
            for (FallbackStore.Entry e : own) {
                byte[] data;
                try {
                    data = store.read(e.path);
                } catch (Exception ex) {
                    log.warn("Could not read fallback file {} ({}); {} file(s) left for next cycle",
                            e.path, ex.toString(), own.size() - files);
                    return;
                }

                FallbackCodec.Decoded decoded;
                try {
                    decoded = FallbackCodec.decode(data);
                } catch (Exception ex) {
                    quarantine(e, ex);
                    continue;
                }

                try {
                    insert(decoded);
                } catch (Exception ex) {
                    log.warn("Replay of fallback file {} failed ({}); {} file(s) left for next cycle",
                            e.path, AccessLogWriter.errorType(ex), own.size() - files);
                    log.debug("Replay failure detail for fallback file {}", e.path, ex);
                    return;
                }

                try {
                    store.delete(e.path);
                    sizeBytes.addAndGet(-e.size);
                } catch (Exception ex) {
                    log.error("Fallback file {} was inserted but could not be deleted ({}); it will be "
                            + "re-inserted next cycle unless removed", e.path, ex.toString());
                    return;
                }
                files++;
                rows += decoded.rows.size();
            }
            if (files > 0) {
                log.info("Fallback recovery replayed {} file(s), {} rows, into ClickHouse", files, rows);
            }
        } catch (Throwable t) {
            log.error("Fallback recovery cycle failed ({})", AccessLogWriter.errorType(t));
            log.debug("Fallback recovery cycle failure detail", t);
        }
    }

    @SuppressWarnings("unchecked")
    private void insert(FallbackCodec.Decoded d) {
        if (d.rows.isEmpty()) return;
        AccessLogWriter w = d.masked ? rawWriter : writer;
        switch (d.type) {
            case ACCESS: w.insertAccessLogBatch(accessTable, (List<AccessLogModel>) d.rows);       break;
            case REST:   w.insertRestAccessLogBatch(restTable, (List<RestAccessLogModel>) d.rows); break;
            default:     w.insertLoginReportBatch(loginTable, (List<UserLoginReportModel>) d.rows); break;
        }
    }

    private boolean ping() {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            try {
                s.setQueryTimeout(10);
            } catch (Exception ignored) {
                // driver may not support it
            }
            s.execute("SELECT 1");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void quarantine(FallbackStore.Entry e, Exception cause) {
        try {
            store.rename(e.path, e.path + ".corrupt");
            log.error("Fallback file {} is unreadable ({}); renamed to .corrupt and skipped", e.path, cause.toString());
        } catch (Exception ex) {
            log.error("Fallback file {} is unreadable ({}) and could not be quarantined ({})",
                    e.path, cause.toString(), ex.toString());
        }
    }

    /**
     * Claims files left by earlier processes of this same host (same instance name, PID no longer
     * alive) by atomically renaming them to this process's instance id. Disk only.
     */
    private void adoptOrphans() throws Exception {
        int adopted = 0;
        for (FallbackStore.Entry e : store.list(instanceName + "-")) {
            String file = e.fileName();
            int us = file.indexOf('_');
            if (us < 0) continue;
            String id   = file.substring(0, us);
            int    dash = id.lastIndexOf('-');
            if (dash < 0 || !id.substring(0, dash).equals(instanceName)) continue;

            long filePid;
            try {
                filePid = Long.parseLong(id.substring(dash + 1));
            } catch (NumberFormatException nfe) {
                continue;
            }
            if (filePid == pid) continue;
            if (ProcessHandle.of(filePid).map(ProcessHandle::isAlive).orElse(false)) continue;

            String rest    = file.substring(us + 1, file.length() - SUFFIX.length());
            String newName = instanceId + "_" + rest + "-p" + filePid + SUFFIX;
            if (store.rename(e.path, e.dir() + "/" + newName)) adopted++;
        }
        if (adopted > 0) {
            log.info("Adopted {} fallback file(s) left by earlier process(es) of '{}'", adopted, instanceName);
        }
    }

    // ---- naming helpers ----

    private static String moduleOf(LogType type, Object row) {
        String module;
        switch (type) {
            case ACCESS: module = ((AccessLogModel) row).getModule();     break;
            case REST:   module = ((RestAccessLogModel) row).getModule(); break;
            default:     module = null;                                   break; // login rows have no module
        }
        return sanitize(module);
    }

    /** Keeps [A-Za-z0-9.-]; no '_' (file-name separator), no leading dots, never empty. */
    static String sanitize(String s) {
        if (s == null) return UNKNOWN;
        String clean = s.trim().replaceAll("[^A-Za-z0-9.-]", "-").replaceAll("^\\.+", "");
        if (clean.length() > 100) clean = clean.substring(0, 100);
        return clean.isEmpty() ? UNKNOWN : clean;
    }

    private static String hostName() {
        String h = System.getenv("HOSTNAME");
        if (h == null || h.trim().isEmpty()) h = System.getenv("COMPUTERNAME");
        if (h == null || h.trim().isEmpty()) {
            try {
                h = InetAddress.getLocalHost().getHostName();
            } catch (Exception ignored) {
                h = null;
            }
        }
        return (h == null || h.trim().isEmpty()) ? shortRandom() : h;
    }

    private static long currentPid() {
        try {
            return ProcessHandle.current().pid();
        } catch (Throwable t) {
            return -1L;
        }
    }

    private static String shortRandom() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}
