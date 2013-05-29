package nl.renarj.jasdb.storage.transactional;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;

import java.io.File;

/**
 * @author Renze de Vries
 */
public class TransactionalRecordWriterFactory implements RecordWriterFactory {
    @Override
    public RecordWriter createWriter(File file) throws JasDBStorageException {
        RecordWriter recordWriter = new TransactionalRecordWriter(file);
        return recordWriter;
    }
}
