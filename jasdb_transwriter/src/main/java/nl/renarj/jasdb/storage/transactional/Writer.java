package nl.renarj.jasdb.storage.transactional;

import nl.renarj.jasdb.core.exceptions.DatastoreException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Renze de Vries
 */
public interface Writer {

    void openWriter() throws DatastoreException;

    boolean isOpen();

    void closeWriter() throws JasDBStorageException;

    RecordResultImpl readRecord(Supplier<Optional<Long>> recordPointerSupplier) throws DatastoreException;

    Long writeRecord(String recordContents, Consumer<Long> postWriteAction) throws DatastoreException;

    void removeRecord(Supplier<Optional<Long>> recordPointerSupplier, Consumer<Long> postRemoveAction) throws DatastoreException;

    Long updateRecord(String recordContents, Supplier<Optional<Long>> recordPointerSupplier, BiConsumer<Long, Long> consumer) throws DatastoreException;

    long getDiskSize() throws JasDBStorageException;

    long getSize();

    RecordIterator readAllRecords() throws DatastoreException;

    RecordIterator readAllRecords(int limit) throws DatastoreException;
}
