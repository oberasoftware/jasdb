package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.api.storage.RecordWriterFactory;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;

import java.io.File;

/**
 * @author Renze de Vries
 */
public class BTreeRecordWriterFactory implements RecordWriterFactory<UUIDKey> {
    @Override
    public String providerName() {
        return "inmemory";
    }

    @Override
    public RecordWriter<UUIDKey> createWriter(File file) throws JasDBStorageException {
        return new BtreeIndexRecordWriter(file);
    }
}
