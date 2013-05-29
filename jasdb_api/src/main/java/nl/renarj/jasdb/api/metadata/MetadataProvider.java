package nl.renarj.jasdb.api.metadata;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface MetadataProvider {
    void setMetadataStore(MetadataStore metadataStore);

    String getMetadataType();

    void registerMetadataEntity(SimpleEntity entity, long recordPointer) throws JasDBStorageException;
}
