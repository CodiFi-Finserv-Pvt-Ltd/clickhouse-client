package in.codifi.clickhouse.logs.fallback;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local-filesystem store under {@code clickhouse.fallback.disk.base.path}. The base directory must
 * already exist (the operator creates it); sub-folders are created on demand.
 */
public class DiskFallbackStore implements FallbackStore {

    private static final String SUFFIX = ".jsonl";

    private final Path base;

    public DiskFallbackStore(String basePath) {
        Path p = Paths.get(basePath).toAbsolutePath().normalize();
        if (!Files.isDirectory(p)) {
            throw new IllegalArgumentException("disk base path does not exist or is not a directory: " + p);
        }
        if (!Files.isWritable(p)) {
            throw new IllegalArgumentException("disk base path is not writable: " + p);
        }
        this.base = p;
    }

    @Override
    public void write(String path, byte[] data) throws IOException {
        Path target = resolve(path);
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.write(tmp, data, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            move(tmp, target);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // best effort
            }
            throw e;
        }
    }

    @Override
    public List<Entry> list(String fileNamePrefix) throws IOException {
        List<Entry> out = new ArrayList<>();
        // <base>/<log-type>/<module>/<file>  → depth 3
        try (Stream<Path> stream = Files.walk(base, 3)) {
            for (Path p : stream.collect(Collectors.toList())) {
                String name = p.getFileName().toString();
                if (!name.endsWith(SUFFIX) || !name.startsWith(fileNamePrefix)) continue;
                try {
                    if (!Files.isRegularFile(p)) continue;
                    out.add(new Entry(relative(p), Files.size(p)));
                } catch (NoSuchFileException ignored) {
                    // deleted/claimed between walk and stat
                }
            }
        }
        return out;
    }

    @Override
    public byte[] read(String path) throws IOException {
        return Files.readAllBytes(resolve(path));
    }

    @Override
    public void delete(String path) throws IOException {
        Files.deleteIfExists(resolve(path));
    }

    @Override
    public boolean rename(String from, String to) throws IOException {
        try {
            move(resolve(from), resolve(to));
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    @Override
    public boolean supportsAtomicRename() {
        return true;
    }

    @Override
    public String describe() {
        return "disk:" + base;
    }

    private static void move(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to);
        }
    }

    private Path resolve(String path) {
        Path p = base.resolve(path).normalize();
        if (!p.startsWith(base)) {
            throw new IllegalArgumentException("path escapes fallback base: " + path);
        }
        return p;
    }

    private String relative(Path p) {
        return base.relativize(p).toString().replace('\\', '/');
    }
}
