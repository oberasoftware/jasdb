package nl.renarj.jasdb.api.serializer;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;

import java.io.InputStream;

/**
 * @author Renze de Vries
 */
public interface EntityDeserializer {
    SimpleEntity deserializeEntity(String entity) throws MetadataParseException;

    SimpleEntity deserializeEntity(InputStream stream) throws MetadataParseException;
}
