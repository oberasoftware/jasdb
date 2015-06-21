package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;

import java.lang.reflect.Method;

/**
 * @author Renze de Vries
 */
public class PropertyMetadataImpl implements PropertyMetadata {
    private final TypeMapper typeMapper;
    private final Method readMethod;
    private final Method writeMethod;
    private final String propertyName;
    private final boolean key;

    public PropertyMetadataImpl(TypeMapper typeMapper, Method readMethod, Method writeMethod, String propertyName, boolean key) {
        this.typeMapper = typeMapper;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.propertyName = propertyName;
        this.key = key;
    }

    @Override
    public TypeMapper getTypeMapper() {
        return typeMapper;
    }

    @Override
    public boolean isKey() {
        return key;
    }

    @Override
    public Method getReadMethod() {
        return readMethod;
    }

    @Override
    public Method getWriteMethod() {
        return writeMethod;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String toString() {
        return "PropertyMetadataImpl{" +
                "typeMapper=" + typeMapper +
                ", readMethod=" + readMethod +
                ", writeMethod=" + writeMethod +
                ", propertyName='" + propertyName + '\'' +
                ", key=" + key +
                '}';
    }
}
