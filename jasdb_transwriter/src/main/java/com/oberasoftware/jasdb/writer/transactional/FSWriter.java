package com.oberasoftware.jasdb.writer.transactional;

import com.oberasoftware.jasdb.api.exceptions.DatastoreException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RecordStoreInUseException;
import com.oberasoftware.jasdb.api.exceptions.RecordNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author Renze de Vries
 */
public class FSWriter implements Writer {
    private static final Logger LOG = LoggerFactory.getLogger(FSWriter.class);

    private static final int HEADER_SIZE = 64;
    private static final String DATA_ENCODING = "UTF-8";
    private static final int LONG_BYTE_SIZE = Long.SIZE / Byte.SIZE;
    private static final int HEADER_RECORD_FLAG = LONG_BYTE_SIZE * 2;
    private static final int BUFFER_SIZE = 4096;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

    private static final long RECORD_VERSIONS = 1;
    private static final int LONGSIZE = LONG_BYTE_SIZE;

    private static final int RECORD_HEADER_SIZE = 20;
    private static final int RESERVE_SPACE_PCT = 20;

    private File recordLocation;
    private FileLock fileLock;
    private FileChannel channel;
    private RandomAccessFile randomAccess;

    private Lock lock = new ReentrantLock();

    private long recordPosition = HEADER_SIZE;
    private AtomicLong recordCount = new AtomicLong(0);

    public FSWriter(File recordLocation) {
        this.recordLocation = recordLocation;
    }

    @Override
    public void openWriter() throws DatastoreException {
        try {
            File parentDir = recordLocation.getParentFile();
            checkState(parentDir.exists() || parentDir.mkdirs());

            this.randomAccess = new RandomAccessFile(recordLocation, "rw");
            this.channel = randomAccess.getChannel();
            LOG.debug("Acquiring exclusive file lock on: {}", recordLocation);
            this.fileLock = this.channel.tryLock();
            if(this.fileLock != null) {
                LOG.debug("Got an exclusive lock on: {}", recordLocation);
                if(channel.size() > 0) {
                    loadHeader();
                } else {
                    recordCount = new AtomicLong(0);
                    recordPosition = HEADER_SIZE;

                    writeHeader();
                }
            } else {
                throw new RecordStoreInUseException("Record datastore: " + recordLocation + " is already in use, cannot be opened");
            }
        } catch(IOException e) {
            throw new DatastoreException("Unable to open record store", e);
        } catch(OverlappingFileLockException e) {
            throw new RecordStoreInUseException("Record datastore: " + recordLocation + " is already in use, cannot be opened");
        }
    }

    @Override
    public boolean isOpen() {
        return this.channel != null && this.channel.isOpen();
    }

    @Override
    public void closeWriter() throws JasDBStorageException {
        try {
            if(channel != null) {
                if(this.fileLock != null) {
                    this.fileLock.release();
                }
                this.channel.close();
                this.randomAccess.close();
                this.channel = null;

            }
        } catch(IOException e) {
            throw new DatastoreException("Unable to cleanly close record store", e);
        }
    }

    private void writeHeader() throws DatastoreException {
        lock.lock();
        try {
            headerBuffer.clear();
            headerBuffer.putLong(0, RECORD_VERSIONS);
            headerBuffer.putLong(LONGSIZE, recordCount.get());
            headerBuffer.putLong(LONGSIZE + LONGSIZE, recordPosition);
            channel.write(headerBuffer, 0);
        } catch(IOException e) {
            throw new DatastoreException("Unable to write header information", e);
        } finally {
            lock.unlock();
        }
    }

    private void loadHeader() throws DatastoreException {
        lock.lock();
        try {
            headerBuffer.clear();
            int bytesRead = channel.read(headerBuffer, 0);
            if(bytesRead == HEADER_SIZE) {
                long version = headerBuffer.getLong(0);
                recordCount.set(headerBuffer.getLong(LONGSIZE));
                recordPosition = headerBuffer.getLong(LONGSIZE + LONGSIZE);
                if(version != RECORD_VERSIONS) {
                    throw new DatastoreException("Record version not supported");
                }
            } else {
                throw new DatastoreException("Invalid record header information");
            }
        } catch(IOException e) {
            throw new DatastoreException("Unable to read header information", e);
        }

        finally {
            lock.unlock();
        }
    }

    private void incrementRecordCount() throws DatastoreException {
        recordCount.incrementAndGet();
        writeHeader();
    }

    private void decrementRecordCount() throws DatastoreException {
        recordCount.decrementAndGet();
        writeHeader();
    }

    @Override
    public long getDiskSize() throws JasDBStorageException {
        try {
            return channel.size();
        } catch(IOException e) {
            throw new DatastoreException("Unable to determine disksize");
        }
    }

    @Override
    public long getSize() {
        return recordCount.get();
    }

    @Override
    public RecordIteratorImpl readAllRecords() throws DatastoreException {
        return readAllRecords(-1);
    }

    @Override
    public RecordIteratorImpl readAllRecords(int limit) throws DatastoreException {
        return new RecordIteratorImpl(this, HEADER_SIZE, limit);
    }

    @Override
    public RecordResultImpl readRecord(Supplier<Optional<Long>> recordPointerSupplier) throws DatastoreException {
        try {
            long recordPosition = recordPointerSupplier.get().orElseThrow(() -> new RecordNotFoundException("Unable to read record, could not be found"));
            LOG.debug("Reading record at position: {}", recordPosition);

            ByteBuffer recordPointer = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = channel.read(recordPointer, recordPosition);
            long currentPosition = recordPosition + RECORD_HEADER_SIZE;

            if(read != -1) {
                long recordSize = recordPointer.getLong(0);
                long extraSpace = recordPointer.getLong(LONG_BYTE_SIZE);
                int intFlag = recordPointer.getInt(HEADER_RECORD_FLAG);
                RECORD_FLAG flag = RECORD_FLAG.getRecordFlag(intFlag);
                if(flag == RECORD_FLAG.ACTIVE || flag == RECORD_FLAG.UPDATED) {
                    long endRecord = currentPosition + recordSize;

                    LOG.trace("Record size: {}", recordSize);
                    LOG.trace("End of record: {}", endRecord);

                    Inflater inflater = new Inflater();
                    StringBuilder outputBuffer = new StringBuilder();

                    ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                    while(currentPosition < endRecord) {
                        readBuffer.clear();

                        int readBytes = channel.read(readBuffer, currentPosition);
                        currentPosition += readBytes;
                        LOG.trace("Reading record, current position: {}", currentPosition);

                        byte[] readInput = readBuffer.array();
                        inflater.setInput(readInput);

                        byte[] buffer = new byte[BUFFER_SIZE];
                        while(!inflater.needsInput() && !inflater.finished()) {

                            try {
                                int bytesUncompressed = inflater.inflate(buffer);
                                outputBuffer.append(new String(buffer, 0, bytesUncompressed, DATA_ENCODING));
                            } catch(DataFormatException e) {
                                throw new DatastoreException("Unable to deflate data store", e);
                            }
                        }
                    }
                    inflater.end();

                    LOG.trace("Record contents: {}", outputBuffer.toString());

                    return new RecordResultImpl(recordPosition, outputBuffer.toString(), recordSize + extraSpace + RECORD_HEADER_SIZE, flag);
                } else {
                    return new RecordResultImpl(recordPosition, null, recordSize + extraSpace + RECORD_HEADER_SIZE, flag);
                }
            } else {
                return new RecordResultImpl(recordPosition, null, -1, RECORD_FLAG.EMPTY);
            }
        } catch(IOException e) {
            throw new DatastoreException("Unable to read record at position: " + recordPosition, e);
        }
    }

    @Override
    public Long writeRecord(String recordContents, Consumer<Long> postAction) throws DatastoreException {
        try {
            lock.lock();
            try {
                long recordStart = recordPosition; //channel.position();
                long currentPosition = recordStart + RECORD_HEADER_SIZE;

                LOG.debug("Record start: {}", recordStart);
                Deflater compresser = new Deflater();
                compresser.setInput(recordContents.getBytes(DATA_ENCODING));
                compresser.finish();

                byte[] buffer = new byte[1024];
                while(!compresser.finished()) {
                    int nrBytes = compresser.deflate(buffer);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, nrBytes);
                    channel.write(byteBuffer, currentPosition);
                    currentPosition += nrBytes;
                }
                long bytesWritten = compresser.getBytesWritten();
                long extraSpace = (long)(bytesWritten * ((double)RESERVE_SPACE_PCT / 100.0));
                compresser.end();

                LOG.debug("Writing {} Bytes", bytesWritten);
                ByteBuffer recordLength = ByteBuffer.allocate(RECORD_HEADER_SIZE);
                recordLength = recordLength.putLong(0, bytesWritten);
                recordLength = recordLength.putLong(LONG_BYTE_SIZE, extraSpace);
                recordLength = recordLength.putInt(HEADER_RECORD_FLAG, RECORD_FLAG.ACTIVE.getFlag());
                channel.write(recordLength, recordStart);
                recordPosition = (currentPosition + extraSpace);

                incrementRecordCount();

                if(postAction != null) {
                    LOG.debug("Executing post write operation for point: {}", recordStart);
                    postAction.accept(recordStart);
                }

                return recordStart;
            } finally {
                lock.unlock();
            }
        } catch(IOException e) {
            throw new DatastoreException("Unable to store record to storage", e);
        }
    }

    @Override
    public void removeRecord(Supplier<Optional<Long>> recordPointerSupplier, Consumer<Long> postRemoveAction) throws DatastoreException {
        lock.lock();
        try {
            long recordPointer = recordPointerSupplier.get().orElseThrow(() -> new RecordNotFoundException("Unable to remove record, could not be found"));

            ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = channel.read(recordHeader, recordPointer);
            if(read != -1) {
                recordHeader.putInt(HEADER_RECORD_FLAG, RECORD_FLAG.DELETED.getFlag());
                recordHeader.flip();
                channel.write(recordHeader, recordPointer);

                LOG.debug("Flagged record: {}  for deletion", recordPointer);
                decrementRecordCount();
            }

            if(postRemoveAction != null) {
                LOG.debug("Executing post remove action for record: {}", recordPointer);
                postRemoveAction.accept(recordPointer);
            }
        } catch(IOException e) {
            LOG.error("Unable to remove record from storage", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Long updateRecord(String recordContents, Supplier<Optional<Long>> recordPointerSupplier, BiConsumer<Long, Long> postUpdateAction) throws DatastoreException {
        lock.lock();
        try {
            long recordPointer = recordPointerSupplier.get().orElseThrow(() -> new RecordNotFoundException("Record not Found, cannot update"));
            long updatedRecordPointer;

            LOG.debug("Starting record update: {}", recordPointer);
            ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            int read = channel.read(recordHeader, recordPointer);
            if(read != -1) {
                long recordSize = recordHeader.getLong(0);
                long reservedSpace = recordHeader.getLong(LONG_BYTE_SIZE);
                ByteBuffer recordLength = ByteBuffer.allocate(RECORD_HEADER_SIZE);
                recordLength = recordLength.putLong(0, recordSize);
                recordLength = recordLength.putLong(LONG_BYTE_SIZE, reservedSpace);
                recordLength = recordLength.putInt(HEADER_RECORD_FLAG, RECORD_FLAG.UPDATED.getFlag());
                channel.write(recordLength, recordPointer);

                recordCount.decrementAndGet();

                updatedRecordPointer = writeRecord(recordContents, null);
            } else {
                LOG.debug("Record update for: {} not possible, reinstering at new position", recordPointer);
                updatedRecordPointer = writeRecord(recordContents, null);
            }

            if(postUpdateAction != null) {
                LOG.debug("Executing post record writer update action old pointer: {} new pointer: {}", recordPointer, updatedRecordPointer);
                postUpdateAction.accept(recordPointer, updatedRecordPointer);
            }

            return updatedRecordPointer;
        } catch(IOException e) {
            throw new DatastoreException("Unable to read/write to storage", e);
        } finally {
            lock.unlock();
        }
    }
}
