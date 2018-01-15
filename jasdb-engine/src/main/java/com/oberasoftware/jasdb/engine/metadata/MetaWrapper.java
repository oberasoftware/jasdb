package com.oberasoftware.jasdb.engine.metadata;

import java.util.UUID;

/**
* @author Renze de Vries
*/
public class MetaWrapper<T> {
    private T metadataObject;
    private UUID key;

    public MetaWrapper(T metadataObject, UUID key) {
        this.metadataObject = metadataObject;
        this.key = key;
    }

    public T getMetadataObject() {
        return metadataObject;
    }

    public void setMetadataObject(T metadataObject) {
        this.metadataObject = metadataObject;
    }

    public UUID getKey() {
        return key;
    }

    public void setKey(UUID key) {
        this.key = key;
    }
}
