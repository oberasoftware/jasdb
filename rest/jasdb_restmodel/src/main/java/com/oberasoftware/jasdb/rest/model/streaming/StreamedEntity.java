package com.oberasoftware.jasdb.rest.model.streaming;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.rest.model.RestEntity;

/**
 * @author Renze de Vries
 */
public class StreamedEntity implements RestEntity {
    private Entity entity;

    public StreamedEntity(Entity entity) {
        this.entity = entity;
    }

    public Entity getEntity() {
        return entity;
    }
}
