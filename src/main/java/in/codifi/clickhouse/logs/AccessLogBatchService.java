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

import in.codifi.clickhouse.logs.fallback.FallbackManager;
import in.codifi.clickhouse.logs.fallback.LogType;
import in.codifi.clickhouse.logs.model.AccessLogModel;
import in.codifi.clickhouse.logs.model.EmailLogModel;
import in.codifi.clickhouse.logs.model.OrderDetailModel;
import in.codifi.clickhouse.logs.model.RestAccessLogModel;
import in.codifi.clickhouse.logs.model.SmsLogModel;
import in.codifi.clickhouse.logs.model.UserLoginReportModel;

public class AccessLogBatchService {

    private static final Logger log = LoggerFactory.getLogger(AccessLogBatchService.class);

    private final AccessLogCache  cache;
    private final AccessLogWriter manager;
    private final int             batchSize;
    private final long            maxBatchBytes;
    private final long            retryBufferMaxBytes;
    private final String          database;
    private final FallbackManager fallback; // null = disabled → in-memory retry buffer only (pre-2.2 behaviour)

    private static final ExecutorService      pool      = Executors.newFixedThreadPool(5);
    private final        ScheduledExecutorService scheduler;

    private final AtomicBoolean flushing            = new AtomicBoolean(false);
    private final AtomicBoolean flushingAccessLog    = new AtomicBoolean(false);
    private final AtomicBoolean flushingLoginReport  = new AtomicBoolean(false);
    private final AtomicBoolean flushingEmailLog     = new AtomicBoolean(false);
    private final AtomicBoolean flushingSmsLog       = new AtomicBoolean(false);
    private final AtomicBoolean flushingOrderDetail  = new AtomicBoolean(false);

    // Retry buffers (guarded by their own monitor)
    private final Deque<FailedBatch<RestAccessLogModel>>   restRetry        = new ArrayDeque<>();
    private final Deque<FailedBatch<AccessLogModel>>       accessRetry      = new ArrayDeque<>();
    private final Deque<FailedBatch<UserLoginReportModel>> loginReportRetry = new ArrayDeque<>();
    private final Deque<FailedBatch<EmailLogModel>>        emailRetry       = new ArrayDeque<>();
    private final Deque<FailedBatch<SmsLogModel>>          smsRetry         = new ArrayDeque<>();
    private final Deque<FailedBatch<OrderDetailModel>>     orderRetry       = new ArrayDeque<>();
    private long restRetryBytes        = 0L;
    private long accessRetryBytes      = 0L;
    private long loginReportRetryBytes = 0L;
    private long emailRetryBytes       = 0L;
    private long smsRetryBytes         = 0L;
    private long orderRetryBytes       = 0L;

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
        this(cache, manager, batchSize, flushIntervalSeconds, database, maxBatchBytes, retryBufferMaxBytes, null);
    }

    /**
     * @param fallback disk/S3 fallback for failed batches, or {@code null} to use only the
     *                 in-memory retry buffer. Rows the fallback cannot store still go to that buffer.
     */
    public AccessLogBatchService(AccessLogCache cache, AccessLogWriter manager, int batchSize,
            int flushIntervalSeconds, String database, long maxBatchBytes, long retryBufferMaxBytes,
            FallbackManager fallback) {
        this.cache               = cache;
        this.manager             = manager;
        this.batchSize           = batchSize;
        this.maxBatchBytes       = maxBatchBytes;
        this.retryBufferMaxBytes = retryBufferMaxBytes;
        this.database            = database;
        this.fallback            = fallback;

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
        scheduler.scheduleAtFixedRate(this::scheduledFlushEmailLog,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::scheduledFlushSmsLog,
                flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::scheduledFlushOrderDetail,
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

    public void addEmailLog(EmailLogModel model) {
        long entryBytes = estimateEmailLogBytes(model);
        boolean shouldFlush;
        synchronized (cache) {
            cache.getEmailLogBatchList().add(model);
            cache.emailQueueBytes += entryBytes;
            shouldFlush = cache.getEmailLogBatchList().size() >= batchSize
                    || cache.emailQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flushEmailLog();
    }

    public void addSmsLog(SmsLogModel model) {
        long entryBytes = estimateSmsLogBytes(model);
        boolean shouldFlush;
        synchronized (cache) {
            cache.getSmsLogBatchList().add(model);
            cache.smsQueueBytes += entryBytes;
            shouldFlush = cache.getSmsLogBatchList().size() >= batchSize
                    || cache.smsQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flushSmsLog();
    }

    public void addOrderDetail(OrderDetailModel model) {
        long entryBytes = estimateOrderDetailBytes(model);
        boolean shouldFlush;
        synchronized (cache) {
            cache.getOrderDetailBatchList().add(model);
            cache.orderQueueBytes += entryBytes;
            shouldFlush = cache.getOrderDetailBatchList().size() >= batchSize
                    || cache.orderQueueBytes >= maxBatchBytes;
        }
        if (shouldFlush) flushOrderDetail();
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
                if (fallback == null) {
                    log.error("flush() failed — buffering {} REST entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToRestRetry(copy, copyBytes);
                } else {
                    log.error("flush() failed — routing {} REST entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    fallback.logOutageRows(LogType.REST, copy);
                    List<RestAccessLogModel> left = fallback.offer(LogType.REST, copy);
                    if (!left.isEmpty()) addToRestRetry(left, left == copy ? copyBytes : restBytes(left));
                }
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
                if (fallback == null) {
                    log.error("flushAccessLog() failed — buffering {} access entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToAccessRetry(copy, copyBytes);
                } else {
                    log.error("flushAccessLog() failed — routing {} access entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    fallback.logOutageRows(LogType.ACCESS, copy);
                    List<AccessLogModel> left = fallback.offer(LogType.ACCESS, copy);
                    if (!left.isEmpty()) addToAccessRetry(left, left == copy ? copyBytes : accessBytes(left));
                }
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
                if (fallback == null) {
                    log.error("flushLoginReport() failed — buffering {} login-report entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToLoginReportRetry(copy, copyBytes);
                } else {
                    // Login report is never written to the per-row outage INFO stream.
                    log.error("flushLoginReport() failed — routing {} login-report entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    List<UserLoginReportModel> left = fallback.offer(LogType.LOGIN, copy);
                    if (!left.isEmpty()) addToLoginReportRetry(left, left == copy ? copyBytes : loginReportBytes(left));
                }
            } finally {
                flushingLoginReport.set(false);
            }
        });
    }

    // ---- flush: email log ----

    public void flushEmailLog() {
        if (!flushingEmailLog.compareAndSet(false, true)) return;

        List<EmailLogModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<EmailLogModel> list = cache.getEmailLogBatchList();
            if (list.isEmpty()) {
                flushingEmailLog.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.emailQueueBytes;
            list.clear();
            cache.emailQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainEmailRetry();
                manager.insertEmailLogBatch(emailLogTable(), copy);
            } catch (Exception e) {
                if (fallback == null) {
                    log.error("flushEmailLog() failed — buffering {} email entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToEmailRetry(copy, copyBytes);
                } else {
                    // Email log is never written to the per-row outage INFO stream.
                    log.error("flushEmailLog() failed — routing {} email entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    List<EmailLogModel> left = fallback.offer(LogType.EMAIL, copy);
                    if (!left.isEmpty()) addToEmailRetry(left, left == copy ? copyBytes : emailLogBytes(left));
                }
            } finally {
                flushingEmailLog.set(false);
            }
        });
    }

    // ---- flush: sms log ----

    public void flushSmsLog() {
        if (!flushingSmsLog.compareAndSet(false, true)) return;

        List<SmsLogModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<SmsLogModel> list = cache.getSmsLogBatchList();
            if (list.isEmpty()) {
                flushingSmsLog.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.smsQueueBytes;
            list.clear();
            cache.smsQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainSmsRetry();
                manager.insertSmsLogBatch(smsLogTable(), copy);
            } catch (Exception e) {
                if (fallback == null) {
                    log.error("flushSmsLog() failed — buffering {} sms entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToSmsRetry(copy, copyBytes);
                } else {
                    // SMS log is never written to the per-row outage INFO stream.
                    log.error("flushSmsLog() failed — routing {} sms entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    List<SmsLogModel> left = fallback.offer(LogType.SMS, copy);
                    if (!left.isEmpty()) addToSmsRetry(left, left == copy ? copyBytes : smsLogBytes(left));
                }
            } finally {
                flushingSmsLog.set(false);
            }
        });
    }

    // ---- flush: order details ----

    public void flushOrderDetail() {
        if (!flushingOrderDetail.compareAndSet(false, true)) return;

        List<OrderDetailModel> copy;
        long copyBytes;
        synchronized (cache) {
            List<OrderDetailModel> list = cache.getOrderDetailBatchList();
            if (list.isEmpty()) {
                flushingOrderDetail.set(false);
                return;
            }
            copy = new ArrayList<>(list);
            copyBytes = cache.orderQueueBytes;
            list.clear();
            cache.orderQueueBytes = 0L;
        }

        pool.execute(() -> {
            try {
                drainOrderRetry();
                manager.insertOrderDetailBatch(orderDetailTable(), copy);
            } catch (Exception e) {
                if (fallback == null) {
                    log.error("flushOrderDetail() failed — buffering {} order entries ({} bytes) ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    addToOrderRetry(copy, copyBytes);
                } else {
                    // Order details are never written to the per-row outage INFO stream.
                    log.error("flushOrderDetail() failed — routing {} order entries ({} bytes) to fallback ({})",
                            copy.size(), copyBytes, AccessLogWriter.errorType(e));
                    List<OrderDetailModel> left = fallback.offer(LogType.ORDER, copy);
                    if (!left.isEmpty()) addToOrderRetry(left, left == copy ? copyBytes : orderDetailBytes(left));
                }
            } finally {
                flushingOrderDetail.set(false);
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

    private void scheduledFlushEmailLog() {
        synchronized (cache) {
            if (cache.getEmailLogBatchList().isEmpty()) return;
        }
        flushEmailLog();
    }

    private void scheduledFlushSmsLog() {
        synchronized (cache) {
            if (cache.getSmsLogBatchList().isEmpty()) return;
        }
        flushSmsLog();
    }

    private void scheduledFlushOrderDetail() {
        synchronized (cache) {
            if (cache.getOrderDetailBatchList().isEmpty()) return;
        }
        flushOrderDetail();
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
                log.error("Retry flush failed for {} REST entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
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
                log.error("Retry flush failed for {} access entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
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
                log.error("Retry flush failed for {} login-report entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
                addToLoginReportRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void drainEmailRetry() {
        List<FailedBatch<EmailLogModel>> pending;
        synchronized (emailRetry) {
            if (emailRetry.isEmpty()) return;
            pending = new ArrayList<>(emailRetry);
            emailRetry.clear();
            emailRetryBytes = 0L;
        }
        for (FailedBatch<EmailLogModel> batch : pending) {
            try {
                manager.insertEmailLogBatch(emailLogTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} email entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
                addToEmailRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void drainSmsRetry() {
        List<FailedBatch<SmsLogModel>> pending;
        synchronized (smsRetry) {
            if (smsRetry.isEmpty()) return;
            pending = new ArrayList<>(smsRetry);
            smsRetry.clear();
            smsRetryBytes = 0L;
        }
        for (FailedBatch<SmsLogModel> batch : pending) {
            try {
                manager.insertSmsLogBatch(smsLogTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} sms entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
                addToSmsRetry(batch.entries, batch.bytes);
            }
        }
    }

    private void drainOrderRetry() {
        List<FailedBatch<OrderDetailModel>> pending;
        synchronized (orderRetry) {
            if (orderRetry.isEmpty()) return;
            pending = new ArrayList<>(orderRetry);
            orderRetry.clear();
            orderRetryBytes = 0L;
        }
        for (FailedBatch<OrderDetailModel> batch : pending) {
            try {
                manager.insertOrderDetailBatch(orderDetailTable(), batch.entries);
            } catch (Exception e) {
                log.error("Retry flush failed for {} order entries ({} bytes) ({})",
                        batch.entries.size(), batch.bytes, AccessLogWriter.errorType(e));
                addToOrderRetry(batch.entries, batch.bytes);
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

    private void addToEmailRetry(List<EmailLogModel> entries, long bytes) {
        synchronized (emailRetry) {
            emailRetry.addLast(new FailedBatch<>(entries, bytes));
            emailRetryBytes += bytes;
            while (emailRetryBytes > retryBufferMaxBytes && !emailRetry.isEmpty()) {
                FailedBatch<EmailLogModel> oldest = emailRetry.pollFirst();
                emailRetryBytes -= oldest.bytes;
                log.warn("Email retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    private void addToSmsRetry(List<SmsLogModel> entries, long bytes) {
        synchronized (smsRetry) {
            smsRetry.addLast(new FailedBatch<>(entries, bytes));
            smsRetryBytes += bytes;
            while (smsRetryBytes > retryBufferMaxBytes && !smsRetry.isEmpty()) {
                FailedBatch<SmsLogModel> oldest = smsRetry.pollFirst();
                smsRetryBytes -= oldest.bytes;
                log.warn("SMS retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    private void addToOrderRetry(List<OrderDetailModel> entries, long bytes) {
        synchronized (orderRetry) {
            orderRetry.addLast(new FailedBatch<>(entries, bytes));
            orderRetryBytes += bytes;
            while (orderRetryBytes > retryBufferMaxBytes && !orderRetry.isEmpty()) {
                FailedBatch<OrderDetailModel> oldest = orderRetry.pollFirst();
                orderRetryBytes -= oldest.bytes;
                log.warn("Order retry buffer over cap ({} bytes) — dropped {} entries ({} bytes)",
                        retryBufferMaxBytes, oldest.entries.size(), oldest.bytes);
            }
        }
    }

    // ---- table names ----

    private String restTable()        { return database + ".tbl_rest_access_log"; }
    private String accessTable()      { return database + ".tbl_access_log"; }
    private String loginReportTable() { return database + ".tbl_user_loggedin_report"; }
    private String emailLogTable()    { return database + ".tbl_email_logs"; }
    private String smsLogTable()      { return database + ".tbl_sms_logs"; }
    private String orderDetailTable() { return database + ".tbl_order_details"; }

    // ---- byte estimation ----

    private static long estimateBytes(String req, String res) {
        return (req != null ? req.length() : 0L) + (res != null ? res.length() : 0L);
    }

    private static long restBytes(List<RestAccessLogModel> list) {
        long total = 0L;
        for (RestAccessLogModel m : list) total += estimateBytes(m.getReqBody(), m.getResBody());
        return total;
    }

    private static long accessBytes(List<AccessLogModel> list) {
        long total = 0L;
        for (AccessLogModel m : list) total += estimateBytes(m.getReqBody(), m.getResBody());
        return total;
    }

    private static long loginReportBytes(List<UserLoginReportModel> list) {
        long total = 0L;
        for (UserLoginReportModel m : list) total += estimateLoginReportBytes(m);
        return total;
    }

    private static long estimateLoginReportBytes(UserLoginReportModel model) {
        long bytes = 4L; // visitors (Int32)
        bytes += model.getUserId() != null ? model.getUserId().length() : 0L;
        bytes += model.getSource() != null ? model.getSource().length() : 0L;
        bytes += model.getVendor() != null ? model.getVendor().length() : 0L;
        return bytes;
    }

    private static long emailLogBytes(List<EmailLogModel> list) {
        long total = 0L;
        for (EmailLogModel m : list) total += estimateEmailLogBytes(m);
        return total;
    }

    private static long estimateEmailLogBytes(EmailLogModel m) {
        return len(m.getUserId()) + len(m.getEmail()) + len(m.getOtp()) + len(m.getEmailHost())
                + len(m.getEmailPort()) + len(m.getEmailFrom()) + len(m.getEmailSubject())
                + len(m.getEmailContent()) + len(m.getEmailResponse()) + len(m.getEmailStatus())
                + len(m.getCreatedBy()) + len(m.getSource()) + len(m.getFiller());
    }

    private static long smsLogBytes(List<SmsLogModel> list) {
        long total = 0L;
        for (SmsLogModel m : list) total += estimateSmsLogBytes(m);
        return total;
    }

    private static long estimateSmsLogBytes(SmsLogModel m) {
        return len(m.getUserId()) + len(m.getMobile()) + len(m.getOtp()) + len(m.getUrl())
                + len(m.getRequestMessage()) + len(m.getResponseMessage()) + len(m.getSentStatus())
                + len(m.getCreatedBy()) + len(m.getSource()) + len(m.getFiller());
    }

    private static long orderDetailBytes(List<OrderDetailModel> list) {
        long total = 0L;
        for (OrderDetailModel m : list) total += estimateOrderDetailBytes(m);
        return total;
    }

    private static long estimateOrderDetailBytes(OrderDetailModel m) {
        return len(m.getUserId()) + len(m.getAccId()) + len(m.getUser()) + len(m.getToken())
                + len(m.getExchange()) + len(m.getTradingSymbol()) + len(m.getAction())
                + len(m.getTransType()) + len(m.getProduct()) + len(m.getValidity()) + len(m.getPrcType())
                + len(m.getOrderType()) + len(m.getCustomerFirm()) + len(m.getQty()) + len(m.getPrice())
                + len(m.getTriggerPrice()) + len(m.getDiscQty()) + len(m.getCancelledSize())
                + len(m.getMktProtectionPrice()) + len(m.getOrderNo()) + len(m.getTarget())
                + len(m.getStopLossValue()) + len(m.getTrailingStopLoss()) + len(m.getTickSize())
                + len(m.getLtpOrAtp()) + len(m.getSqrOffAbsOrTick()) + len(m.getSlabsOrTick())
                + len(m.getTrailTickYesNo()) + len(m.getClassification()) + len(m.getVendorCode())
                + len(m.getFlagToAddCategory()) + len(m.getOrderCriteria()) + len(m.getPosSqrOffFlag())
                + len(m.getAlgoId()) + len(m.getOrderTag()) + len(m.getComments()) + len(m.getSource())
                + len(m.getMethod()) + len(m.getModule()) + len(m.getDeviceIp()) + len(m.getDeviceId())
                + len(m.getStatus()) + len(m.getRejReason()) + len(m.getCreatedBy())
                + len(m.getActiveStatus()) + len(m.getAppCode()) + len(m.getSourcePlatform())
                + len(m.getFiller1()) + len(m.getFiller2()) + len(m.getFiller3()) + len(m.getFiller4());
    }

    private static long len(String s) {
        return s != null ? s.length() : 0L;
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
        flushEmailLog();
        flushSmsLog();
        flushOrderDetail();
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
        if (fallback != null) fallback.shutdown();
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
