package com.oberasoftware.jasdb.cluster.api;

/**
 * @author Renze de Vries
 */
public interface DistributedLock {
    void lock();

    void unlock();
}
