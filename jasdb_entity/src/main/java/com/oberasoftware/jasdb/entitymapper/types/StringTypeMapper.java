package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import nl.renarj.jasdb.api.properties.MultivalueProperty;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.StringValue;
import nl.renarj.jasdb.api.properties.Value;

/**
 * @author Renze de Vries
 */
public class StringTypeMapper implements TypeMapper<String> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.equals(String.class);
    }

    @Override
    public String mapToRawType(Object value) {
        return value.toString();
    }

    @Override
    public Value mapToValue(Object value) {
        return new StringValue(mapToRawType(value));
    }

    @Override
    public Object mapToEmptyValue() {
        return null;
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);

        property.addValue(mapToValue(value));

        return property;
    }

    @Override
    public Object mapFromProperty(Property property) {
        return property.getFirstValueObject();
    }
}
