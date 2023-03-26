package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.LongValue;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Component
public class LongTypeMapper implements TypeMapper<Long> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.equals(Long.TYPE) || type.equals(Integer.TYPE);
    }

    @Override
    public Long mapToRawType(Class targetClass, Object value) {
        if(value instanceof Long) {
            return (Long)value;
        } else if(value instanceof Integer) {
            int v = (Integer) value;
            return (long) v;
        } else {
            throw new RuntimeJasDBException("Unable to map value: " + value);
        }
    }

    @Override
    public Object mapToEmptyValue() {
        return null;
    }

    @Override
    public Value mapToValue(Object value) {
        return new LongValue(mapToRawType(Long.class, value));
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);
        property.addValue(mapToValue(value));
        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        return property.getFirstValueObject();
    }
}
