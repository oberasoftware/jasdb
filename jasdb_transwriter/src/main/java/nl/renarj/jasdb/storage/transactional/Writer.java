package nl.renarj.jasdb.storage.transactional;

import nl.renarj.jasdb.core.exceptions.DatastoreException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;

/**
 * @author Renze de Vries
 */
public interface Writer {
    void openWriter() throws DatastoreException;

    boolean isOpen();

    void closeWriter() throws JasDBStorageException;

    RecordResultImpl readRecord(long recordPosition) throws DatastoreException;

    Long writeRecord(String recordContents) throws DatastoreException;

    void removeRecord(Long recordPointer) throws DatastoreException;

    Long updateRecord(String recordContents, Long recordPointer) throws DatastoreException;

    long getDiskSize() throws JasDBStorageException;

    long getSize();

    RecordIterator readAllRecords() throws DatastoreException;

    RecordIterator readAllRecords(int limit) throws DatastoreException;
}
