package com.oberasoftware.jasdb.api.entitymapper;

import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.Value;

/**
 * @author Renze de Vries
 */
public interface TypeMapper<T> {
    boolean isSupportedType(Class<?> type);

    T mapToRawType(Object value);

    Value mapToValue(Object value);

    Property mapToProperty(String propertyName, Object value);

    Object mapFromProperty(Property property);
}
