package com.oberasoftware.jasdb.core.serializer;

import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.session.Entity;

import java.io.InputStream;

/**
 * @author Renze de Vries
 */
public interface EntityDeserializer {
    Entity deserializeEntity(String entity) throws MetadataParseException;

    Entity deserializeEntity(InputStream stream) throws MetadataParseException;
}
