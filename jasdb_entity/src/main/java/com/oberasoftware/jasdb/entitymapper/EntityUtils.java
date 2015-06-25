package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class EntityUtils {
    private static final Logger LOG = getLogger(EntityUtils.class);

    private EntityUtils() {

    }

    public static Property map(Object mappableObject, PropertyMetadata propertyMetadata) throws JasDBStorageException {
        TypeMapper typeMapper = propertyMetadata.getTypeMapper();
        LOG.debug("Extracting JasDB property: {} from object: {} using typeMapper: {}", propertyMetadata, mappableObject, typeMapper);

        Object value = getValue(mappableObject, propertyMetadata);
        if(value != null) {
            return typeMapper.mapToProperty(propertyMetadata.getPropertyName(), value);
        } else {
            return null;
        }
    }

    public static Object getValue(Object mappableObject, PropertyMetadata propertyMetadata) throws JasDBStorageException {
        Method readMethod = propertyMetadata.getReadMethod();
        try {
            return readMethod.invoke(mappableObject);
        } catch(InvocationTargetException | IllegalAccessException e) {
            throw new JasDBStorageException("Unable to read property: " + propertyMetadata, e);
        }
    }

    public static UUID toValidUUID(String id) {

        try {
            return UUID.fromString(id);
        } catch(IllegalArgumentException e) {
            return null;
        }
    }
}
