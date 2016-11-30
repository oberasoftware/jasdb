package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.properties.EntityValue;
import nl.renarj.jasdb.api.properties.MultivalueProperty;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.Value;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.oberasoftware.jasdb.entitymapper.types.TypeMapperFactory.getTypeMapper;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class MapEntityMapper implements TypeMapper<Map<String, ?>> {
    private static final Logger LOG = getLogger(MapEntityMapper.class);

    @Override
    public boolean isSupportedType(Class<?> type) {
        return Map.class.isAssignableFrom(type);
    }

    @Override
    public Map<String, ?> mapToRawType(Object value) {
        if(value instanceof Map) {
            return (Map<String, ?>) value;
        } else {
            throw new RuntimeJasDBException("Invalid type, cannot cast object: " + value + " to a map");
        }
    }

    @Override
    public Object mapToEmptyValue() {
        return new HashMap<>();
    }

    @Override
    public Value mapToValue(Object value) {
        return null;
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName, true);
        EmbeddedEntity entity = new EmbeddedEntity();

        Map<String, ?> rawValueMap = mapToRawType(value);
        rawValueMap.forEach((k, v) -> {
            try {
                TypeMapper typeMapper = getTypeMapper(v.getClass());
                entity.addProperty(typeMapper.mapToProperty(k, v));
            } catch (JasDBStorageException e) {
                LOG.error("", e);
            }
        });

        property.addValue(new EntityValue(entity));
        return property;
    }

    @Override
    public Object mapFromProperty(PropertyMetadata propertyMetadata, Property property) {
        EntityValue entityValue = (EntityValue) property.getFirstValue();
        SimpleEntity embeddedEntity = entityValue.getValue();
        List<Property> embeddedProperties = embeddedEntity.getProperties();

        Map<String, Object> properties = new HashMap<>();
        embeddedProperties.forEach(p -> {
            properties.put(p.getPropertyName(), p.getFirstValue().getValue());
        });

        return properties;
    }
}
