package com.oberasoftware.core.concurrency;

import com.oberasoftware.core.exceptions.LockingException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This resource lock manager allows handing out shared locks and exclusive locks, it however
 * can also stop handing out new shared locks when the resource gets invalidated.
 *
 * @author Renze de Vries
 */
public class ResourceLockManager {
    private int sharedLocks;

    private ReentrantLock sharedResourceLock;
    private Semaphore exclusiveResourceLock;
    private boolean resourceValid = true;

    private long ownerThreadId = -1;

    public ResourceLockManager() {
        sharedResourceLock = new ReentrantLock();
        exclusiveResourceLock = new Semaphore(1);
    }

    public void exclusiveLock() {
        if(resourceValid) {
            try {
                exclusiveResourceLock.acquire();
                ownerThreadId = Thread.currentThread().getId();
            } catch(InterruptedException e) {
                throw new RuntimeException("Unable to get writelock", e);
            }
        } else {
            throw new LockingException("Resource is no longer valid, lock not allowed");
        }
    }

    public int getSharedLocks() {
        return sharedLocks;
    }

    public void exclusiveUnlock() {
        ownerThreadId = -1;
        exclusiveResourceLock.release();
    }

    public void exclusiveUnlock(boolean invalidate) {
        if(invalidate) {
            resourceValid = false;
        }
        exclusiveResourceLock.release();
    }

    public void sharedLock() {
        try {
            sharedResourceLock.lock();
            if(sharedLocks == 0) {
                if(ownerThreadId == -1 || Thread.currentThread().getId() != ownerThreadId) {
                    exclusiveResourceLock.acquire();
                }
            }
            try {
                if(resourceValid) {
                    sharedLocks++;
                } else {
                    throw new LockingException("Resource is no longer valid, lock not allowed");
                }
            } finally {
                sharedResourceLock.unlock();
            }
        } catch(InterruptedException e) {
            throw new RuntimeException("Unable to get readlock", e);
        }
    }

    public void sharedUnlock() {
        sharedResourceLock.lock();
        sharedLocks--;
        if(sharedLocks == 0) {
            exclusiveResourceLock.release();
        }

        sharedResourceLock.unlock();
    }

    public boolean isResourceValid() {
        return resourceValid;
    }
}
