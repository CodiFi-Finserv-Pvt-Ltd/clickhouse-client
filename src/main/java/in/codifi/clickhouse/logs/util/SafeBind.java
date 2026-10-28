package in.codifi.clickhouse.logs.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tolerant column binding used by the writers for all three log tables.
 *
 * <p>A field that is missing, throws while being read, or fails to convert is
 * stored as NULL (for {@code Nullable(...)} columns) or the column's
 * zero-value default (for non-nullable columns) instead of aborting the row.
 * Each distinct table+column problem is logged as a WARN exactly once.
 */
public final class SafeBind {

    private static final Logger log = LoggerFactory.getLogger(SafeBind.class);
    private static final Set<String> warned = ConcurrentHashMap.newKeySet();

    private SafeBind() {
    }

    @FunctionalInterface
    public interface ValueSupplier<T> {
        T get() throws Exception;
    }

    /** Non-nullable String column — falls back to "" on a missing/unmappable value. */
    public static void bindString(PreparedStatement ps, int idx, ValueSupplier<String> supplier,
            String table, String column) throws SQLException {
        String value = safeGet(supplier, table, column);
        ps.setString(idx, value != null ? value : "");
    }

    /** Nullable(String) column — falls back to real SQL NULL. */
    public static void bindNullableString(PreparedStatement ps, int idx, ValueSupplier<String> supplier,
            String table, String column) throws SQLException {
        String value = safeGet(supplier, table, column);
        if (value != null) ps.setString(idx, value);
        else                ps.setNull(idx, Types.VARCHAR);
    }

    /** Non-nullable Int32 column — falls back to 0. */
    public static void bindInt(PreparedStatement ps, int idx, ValueSupplier<Integer> supplier,
            String table, String column) throws SQLException {
        Integer value = safeGet(supplier, table, column);
        ps.setInt(idx, value != null ? value : 0);
    }

    /** Nullable(Int32) column — falls back to real SQL NULL. */
    public static void bindNullableInt(PreparedStatement ps, int idx, ValueSupplier<Integer> supplier,
            String table, String column) throws SQLException {
        Integer value = safeGet(supplier, table, column);
        if (value != null) ps.setInt(idx, value);
        else                ps.setNull(idx, Types.INTEGER);
    }

    /** Nullable(Int64) column — falls back to real SQL NULL. */
    public static void bindNullableLong(PreparedStatement ps, int idx, ValueSupplier<Long> supplier,
            String table, String column) throws SQLException {
        Long value = safeGet(supplier, table, column);
        if (value != null) ps.setLong(idx, value);
        else                ps.setNull(idx, Types.BIGINT);
    }

    private static <T> T safeGet(ValueSupplier<T> supplier, String table, String column) {
        try {
            return supplier.get();
        } catch (Exception e) {
            if (warned.add(table + "." + column)) {
                log.warn("field '{}' missing/unmappable for {}, storing NULL", column, table);
            }
            return null;
        }
    }
}
