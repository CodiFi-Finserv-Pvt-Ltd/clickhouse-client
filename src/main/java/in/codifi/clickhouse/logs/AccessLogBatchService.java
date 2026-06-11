package in.codifi.clickhouse.logs;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;

public class AccessLogBatchService {

    private static final Logger log = LoggerFactory.getLogger(AccessLogBatchService.class);

    private final AccessLogCache  cache;
    private final AccessLogWriter manager;
    private final int                batchSize;

    private static final ExecutorService      pool      = Executors.newFixedThreadPool(5);
    private final        ScheduledExecutorService scheduler;

    private final AtomicBoolean flushing           = new AtomicBoolean(false);
    private final AtomicBoolean flushingAccessLog   = new AtomicBoolean(false);

    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize) {
        this(cache, manager, batchSize, 60);
    }

    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize,
            int flushIntervalSeconds) {
        this.cache     = cache;
        this.manager   = manager;
        this.batchSize = batchSize;

        // Daemon scheduler so the JVM can exit without an explicit shutdown() call
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "clickhouse-flush-scheduler");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::scheduledFlush,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::scheduledFlushAccessLog,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Adds a REST access log entry to the batch.
     * Triggers a flush immediately if the batch size threshold is reached.
     */
    public void add(RestAccessLogModel model) {

        model.setTableName(generateTable(model.getInTime()));

        boolean shouldFlush = false;

        synchronized (cache) {
            cache.getBatchList().add(model);
            if (cache.getBatchList().size() >= batchSize) {
                shouldFlush = true;
            }
        }

        if (shouldFlush) {
            flush();
        }
    }

    /**
     * Flushes the batched REST access logs in a thread-safe manner.
     * Prevents concurrent flush execution; processes logs asynchronously.
     */
    public void flush() {

        if (!flushing.compareAndSet(false, true)) {
            return;
        }

        List<RestAccessLogModel> copy;

        try {
            synchronized (cache) {
                List<RestAccessLogModel> list = cache.getBatchList();
                if (list.isEmpty()) {
                    return;
                }
                copy = new ArrayList<>(list);
                list.clear();
            }
        } finally {
            // Flag released only after async work completes (see pool.execute below)
        }

        pool.execute(() -> {
            try {
                Map<String, List<RestAccessLogModel>> grouped = copy.stream()
                        .collect(Collectors.groupingBy(RestAccessLogModel::getTableName));

                for (Map.Entry<String, List<RestAccessLogModel>> entry : grouped.entrySet()) {
                    manager.insertRestAccessLogBatch(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                log.error("flush() - async insert failed", e);
            } finally {
                flushing.set(false);
            }
        });
    }

    /**
     * Adds a general access log entry to the batch.
     * Triggers a flush immediately if the batch size threshold is reached.
     */
    public void addAccessLog(AccessLogModel model) {
        if (model.getInTime() == null) {
            model.setInTime(new Timestamp(System.currentTimeMillis()));
        }
        model.setTableName(generateAccessTable(model.getInTime()));

        boolean shouldAccesslogFlush = false;

        synchronized (cache) {
            cache.getAccessLogBatchList().add(model);
            if (cache.getAccessLogBatchList().size() >= batchSize) {
                shouldAccesslogFlush = true;
            }
        }

        if (shouldAccesslogFlush) {
            flushAccessLog();
        }
    }

    /**
     * Flushes the batched general access logs in a thread-safe manner.
     */
    public void flushAccessLog() {

        if (!flushingAccessLog.compareAndSet(false, true)) {
            return;
        }

        List<AccessLogModel> copyAccessLog;

        try {
            synchronized (cache) {
                List<AccessLogModel> list = cache.getAccessLogBatchList();
                if (list.isEmpty()) {
                    return;
                }
                copyAccessLog = new ArrayList<>(list);
                list.clear();
            }
        } finally {
            // Flag released only after async work completes
        }

        pool.execute(() -> {
            try {
                Map<String, List<AccessLogModel>> grouped = copyAccessLog.stream()
                        .collect(Collectors.groupingBy(AccessLogModel::getTableName));

                for (Map.Entry<String, List<AccessLogModel>> entry : grouped.entrySet()) {
                    manager.insertAccessLogBatch(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                log.error("flushAccessLog() - async insert failed", e);
            } finally {
                flushingAccessLog.set(false);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Scheduled flush callbacks (replaces @Scheduled cron methods)
    // -------------------------------------------------------------------------

    private void scheduledFlush() {
        boolean hasData;
        synchronized (cache) {
            hasData = !cache.getBatchList().isEmpty();
        }
        if (hasData) {
            log.info("Scheduler flushing REST access logs...");
            flush();
        }
    }

    private void scheduledFlushAccessLog() {
        synchronized (cache) {
            if (!cache.getAccessLogBatchList().isEmpty()) {
                flushAccessLog();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Table-name generation — identical format to the original
    // -------------------------------------------------------------------------

    private String generateTable(Timestamp time) {
        Date d    = new Date(time.getTime());
        String date = new SimpleDateFormat("ddMMyyyy").format(d);
        return "tbl_" + date + "_rest_access_log";
    }

    private String generateAccessTable(Timestamp time) {
        Date d    = new Date(time.getTime());
        String date = new SimpleDateFormat("ddMMyyyy").format(d);
        String hour = new SimpleDateFormat("HH").format(d);
        return "tbl_" + date + "_access_log_" + hour;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    void scheduleDaily(Runnable task) {
        scheduler.scheduleAtFixedRate(task, 24, 24, TimeUnit.HOURS);
    }

    /**
     * Stops the scheduler, submits a final flush of both queues,
     * then waits up to 30 s for in-flight inserts to complete.
     */
    public void shutdown() {
        scheduler.shutdown();
        flush();
        flushAccessLog();
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
}
