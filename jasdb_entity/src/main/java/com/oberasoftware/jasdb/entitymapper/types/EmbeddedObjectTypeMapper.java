package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.EntityMapper;
import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.EntityValue;
import com.oberasoftware.jasdb.core.properties.MultivalueProperty;

public class EmbeddedObjectTypeMapper implements TypeMapper<Object> {
    private EntityMapper entityMapper;

    public EmbeddedObjectTypeMapper(EntityMapper entityMapper) {
        this.entityMapper = entityMapper;
    }

    @Override
    public boolean isSupportedType(Class<?> type) {
        return type.getAnnotation(JasDBEntity.class) != null;
    }

    @Override
    public Object mapToEmptyValue() {
        return null;
    }

    @Override
    public Object mapToRawType(Class targetClass, Object value) {
        try {
            return entityMapper.mapFrom(targetClass, (Entity)value);
        } catch (JasDBStorageException e) {
            throw new RuntimeJasDBException("Could not map to Target Entity class", e);
        }
    }

    @Override
    public Value mapToValue(Object value) {
        try {
            var entity = entityMapper.mapTo(value).getJasDBEntity();
            return new EntityValue(entity);
        } catch (JasDBStorageException e) {
            throw new RuntimeJasDBException("Could not map embedded entity", e);
        }

    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);
        var entity = mapToValue(value);
        property.addValue(entity);
        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        Class<?> targetType = propertyMetadata.getReadMethod().getReturnType();
        try {
            return entityMapper.mapFrom(targetType, property.getFirstValueObject());
        } catch (JasDBStorageException e) {
            throw new RuntimeJasDBException("Could not map to Target Entity class", e);
        }
    }
}
