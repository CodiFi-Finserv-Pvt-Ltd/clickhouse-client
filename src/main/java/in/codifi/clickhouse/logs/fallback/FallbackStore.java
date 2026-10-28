package in.codifi.clickhouse.logs.fallback;

import java.util.List;

/**
 * Storage backend for failed batches. Paths are relative to the store's base and always use
 * {@code /} separators: {@code <log-type>/<module>/<file>.jsonl}.
 */
public interface FallbackStore {

    /** Writes a new file. Must be all-or-nothing: a reader never sees a partial file. */
    void write(String path, byte[] data) throws Exception;

    /** Lists {@code .jsonl} files whose file name (last path segment) starts with {@code fileNamePrefix}. */
    List<Entry> list(String fileNamePrefix) throws Exception;

    byte[] read(String path) throws Exception;

    void delete(String path) throws Exception;

    /** Total size of the files {@link #list} returns for this prefix. */
    default long currentSizeBytes(String fileNamePrefix) throws Exception {
        long total = 0L;
        for (Entry e : list(fileNamePrefix)) total += e.size;
        return total;
    }

    /**
     * Atomically renames {@code from} to {@code to}. Returns false if {@code from} no longer exists
     * (e.g. another process claimed it first).
     */
    boolean rename(String from, String to) throws Exception;

    /** True when {@link #rename} is atomic, so two processes can safely race to claim a file. */
    boolean supportsAtomicRename();

    /** Human-readable location for log messages. */
    String describe();

    final class Entry {
        public final String path;
        public final long   size;

        public Entry(String path, long size) {
            this.path = path;
            this.size = size;
        }

        public String fileName() {
            int slash = path.lastIndexOf('/');
            return slash >= 0 ? path.substring(slash + 1) : path;
        }

        public String dir() {
            int slash = path.lastIndexOf('/');
            return slash >= 0 ? path.substring(0, slash) : "";
        }
    }
}
