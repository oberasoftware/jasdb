package com.oberasoftware.jasdb.api.entitymapper;

import com.oberasoftware.jasdb.api.session.Entity;

/**
 * @author Renze de Vries
 */
public interface MapResult {
    EntityMetadata getMetadata();

    Entity getJasDBEntity();

    Object getOriginal();

    String getBagName();
}
