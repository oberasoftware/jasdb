package nl.renarj.jasdb.rest.model.streaming;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.rest.model.RestEntity;

/**
 * @author Renze de Vries
 */
public class StreamedEntity implements RestEntity {
    private SimpleEntity entity;

    public StreamedEntity(SimpleEntity entity) {
        this.entity = entity;
    }

    public SimpleEntity getEntity() {
        return entity;
    }
}
