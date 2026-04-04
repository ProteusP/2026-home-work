package company.vk.edu.distrib.compute.vredakon;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DaoImpl implements Dao<byte[]> {

    private final Logger log = LoggerFactory.getLogger("InMemoryDao");
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private boolean isClosed;

    public DaoImpl() {
        this.isClosed = false;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        return data.getOrDefault(key, null);
    }

    @Override
    public void delete(String key) {
        data.remove(key);
    }

    @Override
    public void upsert(String key, byte[] value) {
        data.put(key, value);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            data.clear();
            isClosed = true;
            return;
        }
        log.error("Resource is already closed");
    }
}
