/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.writer.transactional;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.core.storage.ClonableByteArrayInputStream;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;

import java.io.UnsupportedEncodingException;

public class RecordResultImpl implements RecordResult {
	private long recordSize;
    private long recordPointer;

	private String contents;
	
	private RECORD_FLAG recordFlag;
	
	protected RecordResultImpl(long recordPointer, String contents, long recordSize, RECORD_FLAG recordFlag) {
        this.recordPointer = recordPointer;
		this.contents = contents;
		this.recordSize = recordSize;
		this.recordFlag = recordFlag;
	}

    public long getRecordPointer() {
        return recordPointer;
    }

    public RECORD_FLAG getRecordFlag() {
        return recordFlag;
    }

    private boolean isEmpty() {
		return contents == null || contents.isEmpty() || recordFlag == RECORD_FLAG.DELETED;
	}

    @Override
    public ClonableDataStream getStream() throws JasDBStorageException {
        try {
            return new ClonableByteArrayInputStream(contents.getBytes("UTF8"));
        } catch(UnsupportedEncodingException e) {
            throw new JasDBStorageException("Unable to read record contents, unsupported encoding", e);
        }
    }

    @Override
    public boolean isRecordFound() {
        return !isEmpty();
    }

    @Override
    public long getRecordSize() {
        return recordSize;
    }
}
