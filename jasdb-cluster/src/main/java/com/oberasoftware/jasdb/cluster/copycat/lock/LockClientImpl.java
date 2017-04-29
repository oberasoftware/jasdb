package com.oberasoftware.jasdb.cluster.copycat.lock;

import com.oberasoftware.jasdb.cluster.api.DistributedLock;
import io.atomix.copycat.client.CopycatClient;

/**
 * @author renarj
 */
public class LockClientImpl implements DistributedLock {
    private String lockName;
    private CopycatClient copycatClient;

    public LockClientImpl(String lockName, CopycatClient copycatClient) {
        this.lockName = lockName;
        this.copycatClient = copycatClient;
    }

    @Override
    public void lock() {
        copycatClient.submit(new LockCommand(lockName)).join();
    }

    @Override
    public void unlock() {
        copycatClient.submit(new UnlockCommand(lockName)).join();
    }
}
