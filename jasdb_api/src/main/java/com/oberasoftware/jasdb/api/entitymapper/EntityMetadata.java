package com.oberasoftware.jasdb.api.entitymapper;

import java.util.Map;
import java.util.Optional;

/**
 * @author Renze de Vries
 */
public interface EntityMetadata {
    Class<?> getRawType();

    String getBagName();

    Optional<PropertyMetadata> getKeyProperty();

    Map<String, PropertyMetadata> getProperties();

    PropertyMetadata getProperty(String name);
}
