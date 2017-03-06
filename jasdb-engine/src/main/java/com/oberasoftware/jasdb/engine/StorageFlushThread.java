package com.oberasoftware.jasdb.engine;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Renze de Vries
 */
public class StorageFlushThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StorageFlushThread.class);

    private Map<String, StorageService> storageServiceMap;
    private long interval;

    private volatile boolean running = false;
    private Thread flushThread;

    public StorageFlushThread(Map<String, StorageService> storageServiceMap, long interval) {
        this.storageServiceMap = storageServiceMap;
        this.interval = interval;
    }

    public void start() {
        if(flushThread == null) {
            running = true;
            flushThread = new Thread(this);
            flushThread.start();
        }
    }

    public void stop() throws JasDBStorageException {
        running = false;
        try {
            flushThread.join();
        } catch(InterruptedException e) {
            LOG.info("Waiting for stop command interrupted");
        }
    }

    @Override
    public void run() {
        LOG.info("Starting background flush thread, interval: {}", interval);
        while(running && !flushThread.isInterrupted()) {
            try {
                Thread.sleep(interval);

                runFlush();
            } catch(InterruptedException e) {
                flushThread.interrupt();
                running = false;
            } catch(JasDBStorageException e) {
                LOG.error("Unable to flush index, stopping flush thread", e);

                flushThread.interrupt();
                running = false;
            }
        }
        LOG.info("Finished background flush thread");
    }

    private void runFlush() throws JasDBStorageException {
        for(StorageService storageService : storageServiceMap.values()) {
            LOG.debug("Flushing storage service: {}", storageService);
            storageService.flush();
        }
    }
}
