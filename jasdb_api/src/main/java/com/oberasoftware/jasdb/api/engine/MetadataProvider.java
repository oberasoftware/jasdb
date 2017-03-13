package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;

/**
 * @author Renze de Vries
 */
public interface MetadataProvider {
    void setMetadataStore(MetadataStore metadataStore);

    String getMetadataType();

    void registerMetadataEntity(Entity entity, long recordPointer) throws JasDBStorageException;
}
