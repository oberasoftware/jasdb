package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityMetadata;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import nl.renarj.jasdb.api.SimpleEntity;

/**
 * @author Renze de Vries
 */
public class MapResultImpl implements MapResult {

    private final EntityMetadata metadata;
    private final SimpleEntity entity;
    private final Object original;
    private final String bagName;

    public MapResultImpl(EntityMetadata metadata, SimpleEntity entity, Object original, String bagName) {
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
    public SimpleEntity getJasDBEntity() {
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
