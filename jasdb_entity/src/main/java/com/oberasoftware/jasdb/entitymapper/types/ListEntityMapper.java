package com.oberasoftware.jasdb.entitymapper.types;

import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import nl.renarj.jasdb.api.properties.MultivalueProperty;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.properties.Value;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import org.slf4j.Logger;

import java.util.List;

import static com.oberasoftware.jasdb.entitymapper.types.TypeMapperFactory.getTypeMapper;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class ListEntityMapper implements TypeMapper<List<?>> {
    private static final Logger LOG = getLogger(ListEntityMapper.class);

    @Override
    public boolean isSupportedType(Class<?> type) {
        return List.class.isAssignableFrom(type);
    }

    @Override
    public List<?> mapToRawType(Object value) {
        if(value instanceof List) {
            return (List<?>) value;
        } else {
            throw new RuntimeJasDBException("Invalid type, cannot cast object: " + value + " to a list");
        }
    }

    @Override
    public Value mapToValue(Object value) {
        return null;
    }

    @Override
    public Property mapToProperty(String propertyName, Object value) {
        Property property = new MultivalueProperty(propertyName);
        List<?> values = mapToRawType(value);
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
    public Object mapFromProperty(Property property) {
        return property.getValueObjects();
    }
}
