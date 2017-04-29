package com.oberasoftware.jasdb.cluster.copycat.lock;

import io.atomix.copycat.server.Commit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author renarj
 */
public class LockStateMachine {
    private ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public void lock(Commit<LockCommand> command) {
        LockCommand lockCommand = command.command();
        try {
            locks.putIfAbsent(lockCommand.getName(), new ReentrantLock());
            locks.get(lockCommand.getName()).lock();
        } finally {
            command.close();
        }
    }

    public void unlock(Commit<UnlockCommand> command) {
        UnlockCommand unlockCommand = command.command();
        try {
            locks.computeIfPresent(unlockCommand.getName(), (s, reentrantLock) -> {
                reentrantLock.unlock();
                return null;
            });
        } finally {
            command.close();
        }
    }
}
