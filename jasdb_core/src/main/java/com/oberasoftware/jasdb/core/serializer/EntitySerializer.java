package com.oberasoftware.jasdb.core.serializer;

import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.session.Entity;

/**
 * @author Renze de Vries
 */
public interface EntitySerializer {
    String serializeEntity(Entity entity) throws MetadataParseException;
}
