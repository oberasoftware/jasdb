package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.CacheRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * @author Renze de Vries
 */
public class CacheMonitorThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CacheMonitorThread.class);

    private GlobalCachingMemoryManager cachingMemoryManager;

    private volatile boolean running = true;
    private long interval;
    private long maximumMemory;
    private Thread monitorThread;

    public CacheMonitorThread(GlobalCachingMemoryManager cachingMemoryManager, long interval, long maximumMemory) {
        this.interval = interval;
        this.maximumMemory = maximumMemory;
        this.cachingMemoryManager = cachingMemoryManager;
    }

    public void start() {
        monitorThread = new Thread(this);
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public void stop() {
        running = false;
        try {
            monitorThread.interrupt();
            monitorThread.join();
        } catch(InterruptedException e) {
            LOG.error("Stopping cache monitor was interrupted", e);
        }
    }

    @Override
    public void run() {
        while(running && !Thread.currentThread().isInterrupted()) {
            cachingMemoryManager.checkMemoryState(new HashSet<CacheRegion>());

            try {
                Thread.sleep(interval);
            } catch(InterruptedException e) {
                LOG.debug("Cache monitor interval sleep interrupted");
            }
        }
    }


}
