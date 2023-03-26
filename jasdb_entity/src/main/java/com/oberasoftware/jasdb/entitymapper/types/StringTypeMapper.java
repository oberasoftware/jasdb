package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;
import com.oberasoftware.jasdb.core.properties.StringValue;

/**
 * @author Renze de Vries
 */
public class StringTypeMapper implements TypeMapper<String> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.equals(String.class);
    }

    @Override
    public String mapToRawType(Class targetClass, Object value) {
        return value.toString();
    }

    @Override
    public Value mapToValue(Object value) {
        return new StringValue(mapToRawType(String.class, value));
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
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        return property.getFirstValueObject();
    }
}
