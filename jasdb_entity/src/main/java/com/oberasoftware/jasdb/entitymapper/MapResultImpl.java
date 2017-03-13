package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityMetadata;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import com.oberasoftware.jasdb.api.session.Entity;

/**
 * @author Renze de Vries
 */
public class MapResultImpl implements MapResult {

    private final EntityMetadata metadata;
    private final Entity entity;
    private final Object original;
    private final String bagName;

    public MapResultImpl(EntityMetadata metadata, Entity entity, Object original, String bagName) {
        this.metadata = metadata;
        this.entity = entity;
        this.original = original;
        this.bagName = bagName;
    }

    @Override
    public EntityMetadata getMetadata() {
        return metadata;
    }

    @Override
    public Entity getJasDBEntity() {
        return entity;
    }

    @Override
    public Object getOriginal() {
        return original;
    }

    @Override
    public String getBagName() {
        return bagName;
    }
}
