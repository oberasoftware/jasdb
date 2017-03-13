package com.oberasoftware.jasdb.rest.model.serializers.json.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public class StreamingQueryMonitor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingQueryMonitor.class);

    private static final int DEFAULT_CURSOR_TIMEOUT = 5000;
    private static final int CHECK_INTERVAL = 1000;

    private volatile boolean running = false;
    private Thread monitor;
    private Lock lock = new ReentrantLock();

    private List<StreamableQueryResult> monitoredQueryResults = new LinkedList<>();
    private static final StreamingQueryMonitor instance = new StreamingQueryMonitor();

    public static void registerQueryCurors(StreamableQueryResult qResult) {
        instance.enforceCursorMonitor(qResult);
    }

    private void enforceCursorMonitor(StreamableQueryResult queryResult) {
        lock.lock();
        try {
            monitoredQueryResults.add(queryResult);

            if(!running) {
                startMonitor();
            }
        } finally {
            lock.unlock();
        }

    }

    private void startMonitor() {
        if(!running) {
            running = true;
            monitor = new Thread(this);
            monitor.setDaemon(true);
            monitor.start();
        }
    }

    @Override
    public void run() {
        LOG.debug("Starting query cursor monitor");
        while(running && !Thread.interrupted()) {
            runMonitorCheck();


            try {
                Thread.sleep(CHECK_INTERVAL);
            } catch(InterruptedException e) {
                LOG.info("Sleep interrupted, ending thread");
            }

            lock.lock();
            try {
                if(monitoredQueryResults.isEmpty()) {
                    LOG.debug("Stopping cursor monitor, no open monitors remaining");
                    running = false;
                }
            } finally {
                lock.unlock();
            }

        }
        LOG.debug("Query cursor monitor stopped");
    }

    private void runMonitorCheck() {
        List<StreamableQueryResult> checkMonitors = Collections.emptyList();
        lock.lock();
        try {
            checkMonitors = new ArrayList<>(monitoredQueryResults);
        } finally {
            lock.unlock();
        }

        long currentTime = System.currentTimeMillis();
        for(StreamableQueryResult queryResult : checkMonitors) {
            closeCursor(queryResult, currentTime);
        }
    }

    private void closeCursor(StreamableQueryResult queryResult, long currentTime) {
        if(queryResult.isClosed()) {
            lock.lock();
            try {
                monitoredQueryResults.remove(queryResult);
            } finally {
                lock.unlock();
            }
        }

        long passed = currentTime - queryResult.getLastUsage();
        if(passed > DEFAULT_CURSOR_TIMEOUT) {
            lock.lock();
            try {
                LOG.debug("Closing cursor exceeded open time: {}", passed);
                queryResult.close();
                monitoredQueryResults.remove(queryResult);
            } finally {
                lock.unlock();
            }
        }
    }
}
