package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;

import java.util.UUID;

/**
 * @author Renze de Vries
 */
public interface MetadataProvider {
    String getMetadataType();
//
//    void registerMetadataEntity(Entity entity, UUID metaKey) throws JasDBStorageException;
}
