package nl.renarj.jasdb.storage;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.storage.transactional.TransactionalRecordWriter;

import java.io.File;

public class FSRecordWriterTest extends BaseRecordWriterTest {
    @Override
    protected RecordWriter createRecordWriter(File recordFile) throws JasDBStorageException {
        return new TransactionalRecordWriter(recordFile);
    }
}
