package com.oberasoftware.jasdb.api.entitymapper;

import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;

/**
 * @author Renze de Vries
 */
public interface TypeMapper<T> {
    boolean isSupportedType(Class<?> type);

    Object mapToEmptyValue();

    T mapToRawType(Class targetClass, Object value);

    Value mapToValue(Object value);

    Property mapToProperty(String propertyName, Object value);

    Object mapFromProperty(PropertyMetadata propertyMetadata, Property property);
}
