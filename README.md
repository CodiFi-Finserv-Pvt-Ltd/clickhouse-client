# codifi-clickhouse-logs-core

A reusable, framework-agnostic Java library for writing application logs into **ClickHouse** — batched, non-blocking, resilient, and safe. Built once, used everywhere: add one dependency and your service logs to ClickHouse with consistent behaviour, automatic table creation, and a fallback that keeps your logs even when ClickHouse is down.

- **Non-blocking & never throws** — logging can never slow a request or crash your app.
- **Batched inserts** — efficient bulk writes, not one insert per call.
- **Fallback (disk / S3)** — if ClickHouse is unavailable, logs are saved and automatically recovered.
- **Framework-agnostic** — works with Quarkus 2 (javax) and Quarkus 3 (jakarta); runs on Java 11, 17 and 21.
- **Auto table creation** — all tables are created on startup.

---

## Table of contents

1. [Install](#1-install)
2. [Version history — what each version added](#2-version-history--what-each-version-added)
3. [Quick start](#3-quick-start)
4. [The producer (one-time setup)](#4-the-producer-one-time-setup)
5. [Log types & methods](#5-log-types--methods)
6. [How batching works](#6-how-batching-works)
7. [The fallback mechanism](#7-the-fallback-mechanism)
8. [Configuration reference](#8-configuration-reference)
9. [Setup A — normal JAR (disk fallback)](#9-setup-a--normal-jar-disk-fallback)
10. [Setup B — CI/CD / containers (S3 fallback)](#10-setup-b--cicd--containers-s3-fallback)
11. [Outage logging to OpenSearch](#11-outage-logging-to-opensearch)
12. [The tables](#12-the-tables)
13. [Behaviour & guarantees](#13-behaviour--guarantees)
14. [FAQ](#14-faq)

---

## 1. Install

```xml
<dependency>
    <groupId>io.github.codifi-repositories</groupId>
    <artifactId>codifi-clickhouse-logs-core</artifactId>
    <version>2.3.0</version>
</dependency>
```

You also need a ClickHouse JDBC driver on the classpath:

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.9.1</version>
    <classifier>all</classifier>
    <exclusions>
        <exclusion><groupId>*</groupId><artifactId>*</artifactId></exclusion>
    </exclusions>
</dependency>
<!-- Only if your module hits an ANTLR conflict (e.g. with Hibernate): -->
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>4.13.2</version>
</dependency>
```

> Keep `?compress=0` on the JDBC URL to avoid the driver's LZ4 compression issue.

---

## 2. Version history — what each version added

| Version | What it added |
|---------|---------------|
| **1.0.0 / 1.0.1** | Initial release: access logging into ClickHouse, batching, auto table creation. |
| **2.0.0** | REST access logging (second log type); two partitioned tables (`tbl_access_log`, `tbl_rest_access_log`); configurable batching (size / bytes / time); in-memory retry buffer; materialized `latency_ms`. |
| **2.1.0** | **Login report** (third type, `addLoginReport`) using `SummingMergeTree` for auto-counted logins. **Tolerant mapping (SafeBind)** — a missing/bad field is stored as NULL and never throws. |
| **2.2.0** | **Fallback mechanism** — on an insert failure, batches are written to **disk or S3** and a background task recovers them into ClickHouse when it's back (delete-after-confirm). Configurable, opt-in, non-breaking. |
| **2.3.0** | **Three new types**: email logs (`addEmailLog`), SMS logs (`addSmsLog`), order details (`addOrderDetail`). **OTP masking** for email/SMS. **Decimal(18,6)** numeric fields for orders (string input auto-parsed). **Safer logging** — failure logs never print row data. **Per-row outage logging** to OpenSearch (access/rest, opt-in). |

The current version is **2.3.0** and is fully backward compatible — upgrading changes nothing until you opt into the new features.

---

## 3. Quick start

```java
// 1. inject the logger (produced once, see section 4)
@Inject
ClickHouseAccessLogger clickHouseLogger;

// 2. call the method for your log type
clickHouseLogger.addAccessLog(accessLogModel);   // access log
clickHouseLogger.add(restAccessLogModel);        // REST log
clickHouseLogger.addLoginReport(loginModel);     // login report
clickHouseLogger.addEmailLog(emailModel);        // email log
clickHouseLogger.addSmsLog(smsModel);            // SMS log
clickHouseLogger.addOrderDetail(orderModel);     // order detail
```

Each call queues the row and returns immediately. Tables are created automatically on startup.

---

## 4. The producer (one-time setup)

Create one CDI producer per module. It reads config and builds the logger. This single producer supports **both disk and S3** fallback (config decides which). Use `jakarta.*` imports for Quarkus 3, `javax.*` for Quarkus 2.

```java
@ApplicationScoped
public class ClickHouseLoggerProducer {

    @Inject @DataSource("clickhouse")
    AgroalDataSource dataSource;

    @ConfigProperty(name = "clickhouse.batch.size", defaultValue = "2000") int batchSize;
    @ConfigProperty(name = "clickhouse.flush.interval.seconds", defaultValue = "60") int flushIntervalSeconds;
    @ConfigProperty(name = "clickhouse.masking.enabled", defaultValue = "false") boolean maskingEnabled;
    @ConfigProperty(name = "clickhouse.max.batch.bytes", defaultValue = "10000000") long maxBatchBytes;
    @ConfigProperty(name = "clickhouse.retry.buffer.max.bytes", defaultValue = "50000000") long retryBufferMaxBytes;
    @ConfigProperty(name = "clickhouse.table.retry.seconds", defaultValue = "300") int tableRetrySeconds;

    // fallback: common
    @ConfigProperty(name = "clickhouse.fallback.enabled", defaultValue = "false") boolean fbEnabled;
    @ConfigProperty(name = "clickhouse.fallback.type", defaultValue = "disk") String fbType;
    @ConfigProperty(name = "clickhouse.fallback.max.size.mb", defaultValue = "0") long fbMaxSizeMb;
    @ConfigProperty(name = "clickhouse.fallback.recovery.interval.seconds", defaultValue = "300") int fbRecoverySecs;
    @ConfigProperty(name = "clickhouse.fallback.log.types", defaultValue = "access,rest") String fbLogTypes;
    // fallback: disk
    @ConfigProperty(name = "clickhouse.fallback.disk.base.path", defaultValue = "") String fbDiskPath;
    // fallback: s3
    @ConfigProperty(name = "clickhouse.fallback.s3.endpoint", defaultValue = "") String fbS3Endpoint;
    @ConfigProperty(name = "clickhouse.fallback.s3.bucket", defaultValue = "") String fbS3Bucket;
    @ConfigProperty(name = "clickhouse.fallback.s3.base.path", defaultValue = "") String fbS3BasePath;
    @ConfigProperty(name = "clickhouse.fallback.s3.region", defaultValue = "") String fbS3Region;
    @ConfigProperty(name = "clickhouse.fallback.s3.access.key", defaultValue = "") String fbS3AccessKey;
    @ConfigProperty(name = "clickhouse.fallback.s3.secret.key", defaultValue = "") String fbS3SecretKey;

    private ClickHouseAccessLogger logger;

    @Produces @ApplicationScoped
    public ClickHouseAccessLogger clickHouseLogger() {
        FallbackConfig fb = FallbackConfig.disabled()
            .setEnabled(fbEnabled).setType(fbType)
            .setMaxSizeMb(fbMaxSizeMb)
            .setRecoveryIntervalSeconds(fbRecoverySecs)
            .setLogTypes(fbLogTypes)
            .setDiskBasePath(fbDiskPath)
            .setS3Endpoint(fbS3Endpoint).setS3Bucket(fbS3Bucket)
            .setS3BasePath(fbS3BasePath).setS3Region(fbS3Region)
            .setS3AccessKey(fbS3AccessKey).setS3SecretKey(fbS3SecretKey);

        logger = new ClickHouseAccessLogger(dataSource, batchSize, flushIntervalSeconds,
            true, "logs_db", maskingEnabled, maxBatchBytes, retryBufferMaxBytes,
            tableRetrySeconds, fb);
        return logger;
    }

    @PreDestroy
    void stop() { if (logger != null) logger.shutdown(); }
}
```

You also need a `clickhouse` datasource, e.g.:

```properties
quarkus.datasource.clickhouse.db-kind=other
quarkus.datasource.clickhouse.jdbc.url=jdbc:clickhouse://YOUR_HOST:8123/logs_db?compress=0
quarkus.datasource.clickhouse.jdbc.driver=com.clickhouse.jdbc.ClickHouseDriver
quarkus.datasource.clickhouse.username=default
quarkus.datasource.clickhouse.password=YOUR_PASSWORD
```

> If you only need the stable access/REST logging with no fallback, you can use a shorter constructor without the `FallbackConfig` argument — all older constructors still work.

---

## 5. Log types & methods

| Log type | Method | Table | Since |
|----------|--------|-------|-------|
| Access log | `addAccessLog(AccessLogModel)` | `tbl_access_log` | 1.0 |
| REST access log | `add(RestAccessLogModel)` | `tbl_rest_access_log` | 2.0 |
| Login report | `addLoginReport(UserLoginReportModel)` | `tbl_user_loggedin_report` | 2.1 |
| Email log | `addEmailLog(EmailLogModel)` | `tbl_email_logs` | 2.3 |
| SMS log | `addSmsLog(SmsLogModel)` | `tbl_sms_logs` | 2.3 |
| Order detail | `addOrderDetail(OrderDetailModel)` | `tbl_order_details` | 2.3 |

---

## 6. How batching works

A call does **not** insert immediately — it queues the row in memory and returns. A batch is flushed (bulk-inserted) when the **first** of these is reached:

- **Batch size** (default 2000 rows), or
- **Byte size** (default 10 MB), or
- **Time** (default 60 seconds).

For low-volume types, the 60-second timer usually flushes them — expect up to ~60s before a row appears in ClickHouse. Each type has its own queue and in-memory retry buffer; all share the same settings.

---

## 7. The fallback mechanism

Protects logs when an insert fails for **any** reason (ClickHouse down, timeout, driver error). There is exactly **one active failure path at a time**:

- **Fallback OFF (default):** failed batch held in the in-memory retry buffer; retried; oldest dropped if the buffer fills.
- **Fallback ON:** failed batch written to the configured store (**disk** or **S3**). A background recovery task checks every few minutes; when ClickHouse is reachable it replays each file and deletes it **only after a confirmed insert**.
- **In-memory safety net:** even with fallback ON, if the store can't be used (no size set, over the size limit, write failed, bad config), the batch uses the in-memory buffer — nothing is lost.

Use **disk** for VM/bare-metal deployments (stable local disk). Use **S3** for CI/CD / container deployments (container disks are wiped on restart; S3 is external and durable).

---

## 8. Configuration reference

All keys use the `clickhouse.fallback.` prefix. Nothing here is needed unless you turn fallback on.

| Property | When needed | Notes |
|----------|-------------|-------|
| `enabled` | to use fallback | `true` to turn on. Default `false`. |
| `type` | if enabled | `disk` or `s3`. |
| `max.size.mb` | **mandatory if enabled** | Without it, disk/S3 is NOT used — only in-memory. Caps the store size. |
| `log.types` | optional | Which types use fallback. Default `access,rest`. Add `login,email,sms,order` as needed. |
| `recovery.interval.seconds` | optional | Default 300 (5 min). |
| `instance.name` | optional | Defaults to `HOSTNAME` (pod name in K8s, server name for JARs). |
| `disk.base.path` | **disk only** | Required for `type=disk`. Not needed for S3. |
| `s3.bucket` | **s3 only** | Required for `type=s3`. |
| `s3.region` | **s3 only (AWS)** | e.g. `ap-south-1`. |
| `s3.access.key` / `s3.secret.key` | **s3 only** | Must allow read/write/list/**delete**. |
| `s3.base.path` | s3 optional | Prefix inside the bucket, e.g. `fallback`. |
| `s3.endpoint` | s3 optional | Only for MinIO/compatible. Leave empty for AWS. |
| `outage.loginfo` | optional | `true` prints each access/rest log to a dedicated logger during an outage (for OpenSearch). Default `false`. |

> **Key rule:** `max.size.mb` is mandatory to actually use disk or S3. If you enable fallback but omit it, only the in-memory buffer is used.

---

## 9. Setup A — normal JAR (disk fallback)

Create the base path once (the library creates all sub-folders):

```bash
mkdir -p /opt/clickhouse/logs
chown <app-user> /opt/clickhouse/logs
```

```properties
clickhouse.fallback.enabled=true
clickhouse.fallback.type=disk
clickhouse.fallback.disk.base.path=/opt/clickhouse/logs
clickhouse.fallback.max.size.mb=5120
clickhouse.fallback.recovery.interval.seconds=300
clickhouse.fallback.log.types=access,rest,order
```

Do **not** add any `s3.*` keys for disk.

---

## 10. Setup B — CI/CD / containers (S3 fallback)

Ensure the S3 bucket exists; credentials must allow read/write/list/**delete**.

```properties
clickhouse.fallback.enabled=true
clickhouse.fallback.type=s3
clickhouse.fallback.s3.bucket=logs
clickhouse.fallback.s3.base.path=fallback
clickhouse.fallback.s3.region=ap-south-1
clickhouse.fallback.s3.access.key=YOUR_ACCESS_KEY
clickhouse.fallback.s3.secret.key=YOUR_SECRET_KEY
clickhouse.fallback.max.size.mb=5120
clickhouse.fallback.recovery.interval.seconds=300
clickhouse.fallback.log.types=access,rest,order
clickhouse.fallback.outage.loginfo=true
```

Do **not** set `disk.base.path` for S3. For AWS do **not** set `s3.endpoint` (the SDK builds it from the region; set it only for MinIO).

> **Delete permission** is required (`s3:DeleteObject`) — recovery deletes each file after a confirmed insert; without it, files would be re-inserted every cycle.

---

## 11. Outage logging to OpenSearch

Set `clickhouse.fallback.outage.loginfo=true` (default false), intended for CI/CD builds where logs flow to OpenSearch.

- While ClickHouse is down, each **access and rest** log is printed one row at a time to a dedicated logger (`in.codifi.clickhouse.logs.outage`), so logs reach OpenSearch immediately and survive a pod restart.
- **Access and rest only** — login, email, SMS and order are never printed to this stream.
- **Only during an outage** — normal operation prints nothing.
- The batched write to disk/S3 still happens; this is additional.

---

## 12. The tables

All tables are created automatically in database `logs_db`. The three new ones (2.3.0):

### `tbl_email_logs`
Model `EmailLogModel` — fields: `userId, email, otp, emailHost, emailPort, emailFrom, emailSubject, emailContent, emailResponse, emailStatus, createdBy, source, filler`.
OTP is **masked to `****`** before storage (ClickHouse and fallback files).

```java
EmailLogModel m = new EmailLogModel();
m.setUserId(userId);
m.setEmail(to);
m.setOtp(otp);              // masked automatically
m.setEmailStatus("SUCCESS");
m.setSource("SYSTEM");
clickHouseLogger.addEmailLog(m);
```

### `tbl_sms_logs`
Model `SmsLogModel` — fields: `userId, mobile, otp, url, requestMessage, responseMessage, sentStatus, createdBy, source, filler`. OTP masked the same way.

```java
SmsLogModel m = new SmsLogModel();
m.setUserId(userId);
m.setMobile(mobile);
m.setOtp(otp);             // masked automatically
m.setSentStatus("SENT");
clickHouseLogger.addSmsLog(m);
```

### `tbl_order_details`
Model `OrderDetailModel` — **all fields are String**; numeric fields are parsed to `Decimal(18,6)` on insert (whole or decimal input; a non-numeric value becomes NULL, never an error). Append-only: insert a new row per status change, never update.

```java
OrderDetailModel m = new OrderDetailModel();
m.setUserId(userId);
m.setTradingSymbol(tsym);
m.setExchange(exch);
m.setTransType(transType);
m.setQty("100");           // or "100.50" -> Decimal
m.setPrice("1234.56");     // -> Decimal
m.setOrderNo(orderNo);
m.setStatus("COMPLETE");
m.setSource(source);
clickHouseLogger.addOrderDetail(m);
```

### Filler columns (future fields)
Each new table has filler columns so you can store **new fields in future without changing the table or re-publishing the library**:
- Email / SMS: `filler`.
- Order: `filler1`–`filler4`, plus `source_platform` and `app_code`.

Set them like any field (`m.setFiller1(value)`). They are `Nullable(String)`. Keep a note of what each filler holds (they are not self-describing) until a named column is added.

---

## 13. Behaviour & guarantees

- **Never blocks, never throws** — calls queue and return; logging can't crash or slow the app.
- **Tables auto-created** on startup, retried if ClickHouse is down at boot.
- **Failure logs are safe** — they print table, count, bytes and error type only, never row data (full detail at DEBUG).
- **One mechanism at a time** + delete-after-confirm prevents duplicates.
- **Framework-agnostic** — javax or jakarta; Java 11/17/21.

---

## 14. FAQ

**Do I need a new version to use the new log types?** Use 2.3.0. Older versions don't have email/SMS/order.

**Does enabling fallback change anything if ClickHouse is healthy?** No — fallback only activates on an insert failure.

**Can fallback write to another database?** No — the stores are disk or S3; recovery always targets ClickHouse.

**When does a log appear in ClickHouse?** After the batch flush (up to ~60s for low-volume types). Fallback files appear only during an outage and disappear as they are recovered.

**Is it safe on a busy service?** Yes — inserts are batched on background threads; a call just appends to an in-memory queue.

---

*Maintained by the CodiFi platform team.*
