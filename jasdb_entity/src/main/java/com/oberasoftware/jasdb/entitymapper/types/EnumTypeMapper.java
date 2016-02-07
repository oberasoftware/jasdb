package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import nl.renarj.jasdb.api.properties.MultivalueProperty;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.StringValue;
import nl.renarj.jasdb.api.properties.Value;

/**
 * @author Renze de Vries
 */
public class EnumTypeMapper implements TypeMapper<Enum> {
    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.isEnum();
    }

    @Override
    public Object mapToEmptyValue() {
        return null;
    }

    @Override
    public Enum mapToRawType(Object value) {
        return (Enum) value;
    }

    @Override
    public Value mapToValue(Object value) {
        return new StringValue(((Enum)value).name());
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);
        property.addValue(mapToValue(value));

        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        Class<? extends Enum> propertyType = (Class<? extends Enum>) propertyMetadata.getReadMethod().getReturnType();
        return Enum.valueOf(propertyType, property.getFirstValueObject().toString());
    }
}
