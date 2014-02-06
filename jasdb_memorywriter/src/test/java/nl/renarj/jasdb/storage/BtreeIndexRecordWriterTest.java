package nl.renarj.jasdb.storage;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.storage.btree.BtreeIndexRecordWriter;

import java.io.File;

/**
 * @author Renze de Vries
 */
public class BtreeIndexRecordWriterTest extends BaseRecordWriterTest {
    @Override
    protected RecordWriter createRecordWriter(File recordFile) throws JasDBStorageException {
        return new BtreeIndexRecordWriter(recordFile);
    }
}
