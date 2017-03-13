package com.oberasoftware.jasdb.writer.transactional;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordWriter;

import java.io.File;

public class FSRecordWriterTest extends BaseRecordWriterTest {
    @Override
    protected RecordWriter createRecordWriter(File recordFile) throws JasDBStorageException {
        return new TransactionalRecordWriter(recordFile);
    }
}
