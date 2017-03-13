package com.oberasoftware.jasdb.engine.metadata;

/**
* @author Renze de Vries
*/
public class MetaWrapper<T> {
    private T metadataObject;
    private long recordPointer;

    public MetaWrapper(T metadataObject, long recordPointer) {
        this.metadataObject = metadataObject;
        this.recordPointer = recordPointer;
    }

    public T getMetadataObject() {
        return metadataObject;
    }

    public void setMetadataObject(T metadataObject) {
        this.metadataObject = metadataObject;
    }

    public long getRecordPointer() {
        return recordPointer;
    }

    public void setRecordPointer(long recordPointer) {
        this.recordPointer = recordPointer;
    }
}
