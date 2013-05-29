package nl.renarj.jasdb.api.serializer;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;

/**
 * @author Renze de Vries
 */
public interface EntitySerializer {
    public String serializeEntity(SimpleEntity entity) throws MetadataParseException;
}
