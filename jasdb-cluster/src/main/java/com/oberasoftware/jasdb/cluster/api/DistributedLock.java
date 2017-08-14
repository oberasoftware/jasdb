package com.oberasoftware.jasdb.cluster.api;

/**
 * @author renarj
 */
public interface DistributedLock {
    void lock();

    void unlock();
}
