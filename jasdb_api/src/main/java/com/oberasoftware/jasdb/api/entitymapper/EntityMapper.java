package com.oberasoftware.jasdb.api.entitymapper;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface EntityMapper {
    MapResult mapTo(Object mappableObject) throws JasDBStorageException;

    <T> T mapFrom(Class<T> targetType, SimpleEntity entity) throws JasDBStorageException;
}
