/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.transactional;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.streams.ClonableByteArrayInputStream;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;

import java.io.UnsupportedEncodingException;

public class RecordResultImpl implements RecordResult {
	private long recordSize;
    private long recordPointer;
    private UUIDKey documentId;

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

    public void setRecordPointer(long recordPointer) {
        this.recordPointer = recordPointer;
    }

    protected void setDocumentId(UUIDKey documentId) {
        this.documentId = documentId;
    }

    public RECORD_FLAG getRecordFlag() {
        return recordFlag;
    }

    private boolean isEmpty() {
		return contents == null || contents.isEmpty() || recordFlag == RECORD_FLAG.DELETED;
	}

    @Override
    public UUIDKey getId() {
        return documentId;
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
