package com.oberasoftware.jasdb.api.entitymapper;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;

/**
 * @author Renze de Vries
 */
public interface EntityMapper {
    EntityMetadata getEntityMetadata(Class<?> type) throws JasDBStorageException;

    Object updateId(String id, Object mappableObject) throws JasDBStorageException;

    MapResult mapTo(Object mappableObject) throws JasDBStorageException;

    <T> T mapFrom(Class<T> targetType, Entity entity) throws JasDBStorageException;
}
