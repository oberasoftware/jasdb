package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityMetadata;
import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;

import java.util.Map;
import java.util.Optional;

/**
 * @author Renze de Vries
 */
public class EntityMetadataImpl implements EntityMetadata {
    private final Class<?> rawType;
    private final String bagName;
    private final Optional<PropertyMetadata> keyProperty;
    private final Map<String, PropertyMetadata> properties;

    public EntityMetadataImpl(Class<?> rawType, String bagName, Optional<PropertyMetadata> keyProperty, Map<String, PropertyMetadata> properties) {
        this.rawType = rawType;
        this.bagName = bagName;
        this.keyProperty = keyProperty;
        this.properties = properties;
    }

    @Override
    public Class<?> getRawType() {
        return rawType;
    }

    @Override
    public String getBagName() {
        return bagName;
    }

    @Override
    public Optional<PropertyMetadata> getKeyProperty() {
        return keyProperty;
    }

    @Override
    public Map<String, PropertyMetadata> getProperties() {
        return properties;
    }

    @Override
    public PropertyMetadata getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public String toString() {
        return "EntityMetadataImpl{" +
                "rawType=" + rawType +
                ", bagName='" + bagName + '\'' +
                ", keyProperty='" + keyProperty + '\'' +
                ", properties=" + properties +
                '}';
    }
}
