package com.oberasoftware.jasdb.api.entitymapper;

import nl.renarj.jasdb.api.SimpleEntity;

/**
 * @author Renze de Vries
 */
public interface MapResult {
    EntityMetadata getMetadata();

    SimpleEntity getJasDBEntity();

    Object getOriginal();

    String getBagName();
}
