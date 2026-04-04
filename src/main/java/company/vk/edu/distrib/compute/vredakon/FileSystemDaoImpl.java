package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSystemDaoImpl implements Dao<byte[]> {

    private static final String STORAGE_PATH = System.getProperty("user.dir")
            + "/src/main/java"
            + "/company/vk/edu/distrib/compute/vredakon/storage/";

    private final Logger log = LoggerFactory.getLogger("FileSystemDao");
    private boolean isClosed;

    public FileSystemDaoImpl() {
        this.isClosed = false;
    }

    @Override
    public byte[] get(String key) throws IOException {
        if (!isClosed) {
            return Files.readAllBytes(Path.of(STORAGE_PATH, key));
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        if (!isClosed) {
            Files.write(Path.of(STORAGE_PATH, key), value);
            return;
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void delete(String key) throws IOException {
        if (!isClosed) {
            Files.deleteIfExists(Path.of(STORAGE_PATH, key));
            return;
        }
        throw new IllegalStateException("Resource is closed");
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            isClosed = true;
            return;
        }
        log.error("Resource is already closed");
    }
}
