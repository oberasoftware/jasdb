package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static com.oberasoftware.jasdb.entitymapper.types.TypeMapperFactory.getTypeMapper;

public class SetEntityMapper implements TypeMapper<Set<?>> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return Set.class.isAssignableFrom(type);
    }

    @Override
    public Set<?> mapToRawType(Object value) {
        if(value instanceof Set) {
            return (Set<?>) value;
        } else {
            throw new RuntimeJasDBException("Invalid type, cannot cast object: " + value + " to a Set");
        }
    }

    @Override
    public Object mapToEmptyValue() {
        return new HashSet<>();
    }

    @Override
    public Value mapToValue(Object value) {
        return null;
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName, true);
        Set<?> values = mapToRawType(value);
        values.forEach(v -> {
            try {
                TypeMapper typeMapper = getTypeMapper(v.getClass());
                property.addValue(typeMapper.mapToValue(v));
            } catch (JasDBStorageException e) {
                throw new RuntimeJasDBException("Unable to map list", e);
            }

        });

        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        return new HashSet(property.getValueObjects());
    }
}
