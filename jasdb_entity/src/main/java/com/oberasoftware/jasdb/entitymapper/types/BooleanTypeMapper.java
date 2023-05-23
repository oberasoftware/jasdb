package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.BooleanValue;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;

public class BooleanTypeMapper implements TypeMapper<Boolean> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.equals(Boolean.TYPE);
    }

    @Override
    public Object mapToEmptyValue() {
        return null;
    }

    @Override
    public Boolean mapToRawType(Class targetClass, Object value) {
        return ensureBool(value);
    }

    private Boolean ensureBool(Object value) {
        if(value instanceof Boolean) {
            return (Boolean) value;
        } else if(value instanceof Integer) {
            return ((Integer) value) > 0;
        } else {
            throw new RuntimeJasDBException("Unable to map value: " + value);
        }
    }


    @Override
    public Value mapToValue(Object value) {
        return new BooleanValue(ensureBool(value));
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);
        property.addValue(mapToValue(value));
        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        return ensureBool(property.getFirstValueObject());
    }
}
