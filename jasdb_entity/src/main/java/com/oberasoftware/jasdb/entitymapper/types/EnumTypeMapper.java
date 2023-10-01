package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;
import com.oberasoftware.jasdb.core.properties.StringValue;

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
    public Enum mapToRawType(Class targetClass, Object value) {
        if(targetClass.isEnum()) {
            return Enum.valueOf(targetClass, value.toString());
        } else {
            throw new RuntimeJasDBException("Mappable value is not an enum");
        }
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
