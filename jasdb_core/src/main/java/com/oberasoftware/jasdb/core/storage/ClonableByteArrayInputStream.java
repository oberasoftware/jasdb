package com.oberasoftware.jasdb.core.storage;

import com.oberasoftware.jasdb.api.storage.ClonableDataStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author Renze de Vries
 */
public class ClonableByteArrayInputStream extends ClonableDataStream {
    private byte[] buffer;
    private ByteArrayInputStream inputStream;

    public ClonableByteArrayInputStream(byte[] buffer) {
        this.buffer = buffer;
        this.inputStream = new ByteArrayInputStream(buffer);
    }

    @Override
    public ClonableDataStream clone() throws CloneNotSupportedException {
        return new ClonableByteArrayInputStream(buffer);
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return inputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return inputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        inputStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        inputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        inputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return inputStream.markSupported();
    }
}
