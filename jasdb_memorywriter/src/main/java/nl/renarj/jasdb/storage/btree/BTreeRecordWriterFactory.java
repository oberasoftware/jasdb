package nl.renarj.jasdb.storage.btree;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;

import java.io.File;

/**
 * @author Renze de Vries
 */
public class BTreeRecordWriterFactory implements RecordWriterFactory {
    @Override
    public String providerName() {
        return "inmemory";
    }

    @Override
    public RecordWriter createWriter(File file) throws JasDBStorageException {
        return new BtreeIndexRecordWriter(file);
    }
}
