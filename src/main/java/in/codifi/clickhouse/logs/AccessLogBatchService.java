package in.codifi.clickhouse.logs;

import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;

public class AccessLogBatchService {

    private static final Logger log = LoggerFactory.getLogger(AccessLogBatchService.class);

    private final AccessLogCache  cache;
    private final AccessLogWriter manager;
    private final int             batchSize;
    private final long            maxBatchBytes;
    private final long            retryBufferMaxBytes;
    private final String          database;

    private static final ExecutorService      pool      = Executors.newFixedThreadPool(5);
    private final        ScheduledExecutorService scheduler;

    private final AtomicBoolean flushing            = new AtomicBoolean(false);
    private final AtomicBoolean flushingAccessLog    = new AtomicBoolean(false);
    private final AtomicBoolean flushingLoginReport  = new AtomicBoolean(false);

    // Retry buffers (guarded by their own monitor)
    private final Deque<FailedBatch<RestAccessLogModel>>   restRetry        = new ArrayDeque<>();
    private final Deque<FailedBatch<AccessLogModel>>       accessRetry      = new ArrayDeque<>();
    private final Deque<FailedBatch<UserLoginReportModel>> loginReportRetry = new ArrayDeque<>();
    private long restRetryBytes        = 0L;
    private long accessRetryBytes      = 0L;
    private long loginReportRetryBytes = 0L;

    // ---- constructors ----

    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize) {
        this(cache, manager, batchSize, 60);
    }

    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize,
            int flushIntervalSeconds) {
        this(cache, manager, batchSize, flushIntervalSeconds, "logs_db", 10_000_000L, 50_000_000L);
    }

    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize,
            int flushIntervalSeconds, String database, long maxBatchBytes, long retryBufferMaxBytes) {
        this.cache               = cache;
        this.manager             = manager;
        this.batchSize           = batchSize;
        this.maxBatchBytes       = maxBatchBytes;
        this.retryBufferMaxBytes = retryBufferMaxBytes;
        this.database            = database;

        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "clickhouse-flush-scheduler");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::scheduledFlush,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::scheduledFlushAccessLog,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::scheduledFlushLoginReport,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    // ---- public add ----

    public void add(RestAccessLogModel model) {
        if (model.getInTime() == null) model.setInTime(new Timestamp(System.currentTimeMillis()));

        long entryBytes = estimateBytes(model.getReqBody(), model.getResBody());
        boolean shouldFlush;
        synchronized (cache) {
            cache.getBatchList().add(model);
            cache.restQueueBytes += entryBytes;
            shouldFlush = cache.getBatchList().size() >= batchSize
                    || cache.restQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flush();
    }

    public void addAccessLog(AccessLogModel model) {
        if (model.getInTime() == null) model.setInTime(new Timestamp(System.currentTimeMillis()));

        long entryBytes = estimateBytes(model.getReqBody(), model.getResBody());
        boolean shouldFlush;
        synchronized (cache) {
            cache.getAccessLogBatchList().add(model);
            cache.accessQueueBytes += entryBytes;
            shouldFlush = cache.getAccessLogBatchList().size() >= batchSize
                    || cache.accessQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flushAccessLog();
    }

    public void addLoginReport(UserLoginReportModel model) {
        long entryBytes = estimateLoginReportBytes(model);
        boolean shouldFlush;
        synchronized (cache) {
            cache.getLoginReportBatchList().add(model);
            cache.loginReportQueueBytes += entryBytes;
            shouldFlush = cache.getLoginReportBatchList().size() >= batchSize
                    || cache.loginReportQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flushLoginReport();
    }

    // ---- flush: REST access log ----

    public void flush() {
        if (!flushing.compareAndSet(false, true)) return;

        List<RestAccessLogModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<RestAccessLogModel> list = cache.getBatchList();
            if (list.isEmpty()) {
                flushing.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.restQueueBytes;
            list.clear();
            cache.restQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainRestRetry();
                manager.insertRestAccessLogBatch(restTable(), copy);
            } catch (Exception e) {
                log.error("flush() failed — buffering {} REST entries ({} bytes)", copy.size(), copyBytes, e);
                addToRestRetry(copy, copyBytes);
            } finally {
                flushing.set(false);
            }
        });
    }

    // ---- flush: access log ----

    public void flushAccessLog() {
        if (!flushingAccessLog.compareAndSet(false, true)) return;

        List<AccessLogModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<AccessLogModel> list = cache.getAccessLogBatchList();
            if (list.isEmpty()) {
                flushingAccessLog.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.accessQueueBytes;
            list.clear();
            cache.accessQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainAccessRetry();
                manager.insertAccessLogBatch(accessTable(), copy);
            } catch (Exception e) {
                log.error("flushAccessLog() failed — buffering {} access entries ({} bytes)", copy.size(), copyBytes, e);
                addToAccessRetry(copy, copyBytes);
            } finally {
                flushingAccessLog.set(false);
            }
        });
    }

    // ---- flush: login report ----

    public void flushLoginReport() {
        if (!flushingLoginReport.compareAndSet(false, true)) return;

        List<UserLoginReportModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<UserLoginReportModel> list = cache.getLoginReportBatchList();
            if (list.isEmpty()) {
                flushingLoginReport.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.loginReportQueueBytes;
            list.clear();
            cache.loginReportQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainLoginReportRetry();
                manager.insertLoginReportBatch(loginReportTable(), copy);
            } catch (Exception e) {
                log.error("flushLoginReport() failed — buffering {} login-report entries ({} bytes)", copy.size(), copyBytes, e);
                addToLoginReportRetry(copy, copyBytes);
            } finally {
                flushingLoginReport.set(false);
            }
        });
    }

    // ---- scheduled callbacks ----

    private void scheduledFlush() {
        synchronized (cache) {
            if (cache.getBatchList().isEmpty()) return;
        }
        log.info("Scheduler flushing REST access logs...");
        flush();
    }

    private void scheduledFlushAccessLog() {
        synchronized (cache) {
            if (cache.getAccessLogBatchList().isEmpty()) return;
        }
        flushAccessLog();
    }

    private void scheduledFlushLoginReport() {
        synchronized (cache) {
            if (cache.getLoginReportBatchList().isEmpty()) return;
        }
        flushLoginReport();
    }

    // ---- retry buffer helpers ----

    private void drainRestRetry() {
        List<FailedBatch<RestAccessLogModel>> pending;
        synchronized (restRetry) {
            if (restRetry.isEmpty()) return;
            pending = new ArrayList<>(restRetry);
            restRetry.clear();
            restRetryBytes = 0L;
        }
        for (FailedBatch<RestAccessLogModel> batch : pending) {
            try {
                manager.insertRestAccessLogBatch(restTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} REST entries ({} bytes)", batch.entries.size(), batch.bytes, e);
                addToRestRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void drainAccessRetry() {
        List<FailedBatch<AccessLogModel>> pending;
        synchronized (accessRetry) {
            if (accessRetry.isEmpty()) return;
            pending = new ArrayList<>(accessRetry);
            accessRetry.clear();
            accessRetryBytes = 0L;
        }
        for (FailedBatch<AccessLogModel> batch : pending) {
            try {
                manager.insertAccessLogBatch(accessTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} access entries ({} bytes)", batch.entries.size(), batch.bytes, e);
                addToAccessRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void drainLoginReportRetry() {
        List<FailedBatch<UserLoginReportModel>> pending;
        synchronized (loginReportRetry) {
            if (loginReportRetry.isEmpty()) return;
            pending = new ArrayList<>(loginReportRetry);
            loginReportRetry.clear();
            loginReportRetryBytes = 0L;
        }
        for (FailedBatch<UserLoginReportModel> batch : pending) {
            try {
                manager.insertLoginReportBatch(loginReportTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} login-report entries ({} bytes)", batch.entries.size(), batch.bytes, e);
                addToLoginReportRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void addToRestRetry(List<RestAccessLogModel> entries, long bytes) {
        synchronized (restRetry) {
            restRetry.addLast(new FailedBatch<>(entries, bytes));
            restRetryBytes += bytes;
            while (restRetryBytes > retryBufferMaxBytes && !restRetry.isEmpty()) {
                FailedBatch<RestAccessLogModel> oldest = restRetry.pollFirst();
                restRetryBytes -= oldest.bytes;
                log.warn("REST retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    private void addToAccessRetry(List<AccessLogModel> entries, long bytes) {
        synchronized (accessRetry) {
            accessRetry.addLast(new FailedBatch<>(entries, bytes));
            accessRetryBytes += bytes;
            while (accessRetryBytes > retryBufferMaxBytes && !accessRetry.isEmpty()) {
                FailedBatch<AccessLogModel> oldest = accessRetry.pollFirst();
                accessRetryBytes -= oldest.bytes;
                log.warn("Access retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    private void addToLoginReportRetry(List<UserLoginReportModel> entries, long bytes) {
        synchronized (loginReportRetry) {
            loginReportRetry.addLast(new FailedBatch<>(entries, bytes));
            loginReportRetryBytes += bytes;
            while (loginReportRetryBytes > retryBufferMaxBytes && !loginReportRetry.isEmpty()) {
                FailedBatch<UserLoginReportModel> oldest = loginReportRetry.pollFirst();
                loginReportRetryBytes -= oldest.bytes;
                log.warn("Login-report retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    // ---- table names ----

    private String restTable()        { return database + ".tbl_rest_access_log"; }
    private String accessTable()      { return database + ".tbl_access_log"; }
    private String loginReportTable() { return database + ".tbl_user_loggedin_report"; }

    // ---- byte estimation ----

    private static long estimateBytes(String req, String res) {
        return (req != null ? req.length() : 0L) + (res != null ? res.length() : 0L);
    }

    private static long estimateLoginReportBytes(UserLoginReportModel model) {
        long bytes = 4L; // visitors (Int32)
        bytes += model.getUserId() != null ? model.getUserId().length() : 0L;
        bytes += model.getSource() != null ? model.getSource().length() : 0L;
        bytes += model.getVendor() != null ? model.getVendor().length() : 0L;
        return bytes;
    }

    // ---- lifecycle ----

    void scheduleDaily(Runnable task) {
        scheduler.scheduleAtFixedRate(task, 24, 24, TimeUnit.HOURS);
    }

    public void shutdown() {
        scheduler.shutdown();
        flush();
        flushAccessLog();
        flushLoginReport();
        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Worker pool did not terminate cleanly within 30 s; forcing shutdown.");
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
            log.error("Shutdown interrupted", e);
        }
    }

    // ---- inner type ----

    private static class FailedBatch<T> {
        final List<T> entries;
        final long    bytes;

        FailedBatch(List<T> entries, long bytes) {
            this.entries = entries;
            this.bytes   = bytes;
        }
    }
}
