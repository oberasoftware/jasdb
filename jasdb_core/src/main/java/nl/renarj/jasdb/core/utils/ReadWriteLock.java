package nl.renarj.jasdb.core.utils;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 * Date: 5-6-12
 * Time: 21:16
 */
public class ReadWriteLock {
    private int readers;

    private ReentrantLock readLock;
    private Semaphore writeLock;

    public ReadWriteLock() {
        readLock = new ReentrantLock();
        writeLock = new Semaphore(1);
    }

    public void writeLock() {
        try {
            writeLock.acquire();
        } catch(InterruptedException e) {
            throw new RuntimeException("Unable to get writelock", e);
        }
    }

    public void writeUnlock() {
        writeLock.release();
    }

    public void readLock() {
        try {
            readLock.lock();
            if(readers == 0) {
                writeLock.acquire();
            }
            readers++;
            readLock.unlock();
        } catch(InterruptedException e) {
            throw new RuntimeException("Unable to get readlock", e);
        }
    }

    public void readUnlock() {
        readLock.lock();
        readers--;
        if(readers == 0) {
            writeLock.release();
        }

        readLock.unlock();
    }
}
