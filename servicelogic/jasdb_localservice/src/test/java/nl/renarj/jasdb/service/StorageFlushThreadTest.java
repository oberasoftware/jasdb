package nl.renarj.jasdb.service;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Renze de Vries
 */
public class StorageFlushThreadTest {

    private static final int INTERVAL = 100;

    @Test
    public void testFlushInterval() throws JasDBStorageException, InterruptedException {
        StorageService storageService = mock(StorageService.class);
        Map<String, StorageService> storageServiceMap = new HashMap<>();
        storageServiceMap.put("storageService", storageService);

        StorageFlushThread storageFlushThread = new StorageFlushThread(storageServiceMap, INTERVAL);
        storageFlushThread.start();

        try {
            Thread.sleep(2 * INTERVAL);

            verify(storageService, atLeast(1)).flush();
        } finally {
            storageFlushThread.stop();
        }
    }
}
