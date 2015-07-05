package com.oberasoftware.jasdb.api.entitymapper;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface EntityMapper {
    EntityMetadata getEntityMetadata(Class<?> type) throws JasDBStorageException;

    Object updateId(String id, Object mappableObject) throws JasDBStorageException;

    MapResult mapTo(Object mappableObject) throws JasDBStorageException;

    <T> T mapFrom(Class<T> targetType, SimpleEntity entity) throws JasDBStorageException;
}
