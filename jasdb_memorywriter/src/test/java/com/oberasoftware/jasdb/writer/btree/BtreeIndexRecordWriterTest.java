package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordWriter;

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
