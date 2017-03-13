package com.oberasoftware.jasdb.core.concurrency;

import com.oberasoftware.jasdb.api.exceptions.LockingException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author renarj
 */
public class ResourceLockManagerTest {
    @Test(expected = TimeoutException.class)
    public void testExclusiveLockNotWithSharedLock() throws TimeoutException, InterruptedException, ExecutionException {
        final ResourceLockManager lockManager = new ResourceLockManager();
        lockManager.sharedLock();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Void> exclLock = executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                lockManager.exclusiveLock();
                return null;
            }
        });
        try {
            exclLock.get(100, TimeUnit.MILLISECONDS);
        } finally {
            lockManager.exclusiveUnlock();
            executorService.shutdown();
        }
    }

    @Test
    public void testExclusiveLockCombinedWithSharedLock() throws TimeoutException, InterruptedException, ExecutionException {
        final ResourceLockManager lockManager = new ResourceLockManager();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Void> sharedLock = executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                lockManager.exclusiveLock();
                lockManager.sharedLock();
                return null;
            }
        });
        sharedLock.get(1000, TimeUnit.MILLISECONDS);

        assertTrue(lockManager.isResourceValid());
        assertEquals(1, lockManager.getSharedLocks());
        lockManager.sharedUnlock();
        lockManager.exclusiveUnlock();
        assertEquals(0, lockManager.getSharedLocks());
    }

    @Test
    public void testSharedLock() throws TimeoutException, InterruptedException, ExecutionException {
        final ResourceLockManager lockManager = new ResourceLockManager();
        try {
            lockManager.sharedLock();
            lockManager.sharedLock();
            lockManager.sharedLock();
        } finally {
            lockManager.sharedUnlock();
            lockManager.sharedUnlock();
            lockManager.sharedUnlock();
        }
    }

    @Test
    public void testSharedLockCloseResource() throws TimeoutException, InterruptedException, ExecutionException {
        final ResourceLockManager lockManager = new ResourceLockManager();
        lockManager.sharedLock();
        assertTrue(lockManager.isResourceValid());

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Void> closeResource = executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                lockManager.exclusiveLock();
                lockManager.exclusiveUnlock(true);

                try {
                    lockManager.exclusiveLock();
                    Assert.assertFalse("We expect a lock expection as resource is invalid", true);
                } catch(LockingException e) {
                    //we expect this
                }

                try {
                    lockManager.sharedLock();
                    Assert.assertFalse("We expect a lock expection as resource is invalid", true);
                } catch(LockingException e) {
                    //we expect this
                }
                return null;
            }
        });
        try {
            closeResource.get(100, TimeUnit.MILLISECONDS);
            executorService.shutdown();
            Assert.assertFalse("Close resource should block on shared lock", true);
        } catch(TimeoutException e) {
            //we expect this
        }

        lockManager.sharedUnlock();

        //now it should complete
        try {
            closeResource.get(1000, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }
        Assert.assertFalse(lockManager.isResourceValid());
    }
}
