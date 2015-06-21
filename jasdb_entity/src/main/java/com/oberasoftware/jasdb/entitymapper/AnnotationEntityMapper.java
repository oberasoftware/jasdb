package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityMapper;
import com.oberasoftware.jasdb.api.entitymapper.EntityMetadata;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import com.oberasoftware.jasdb.api.entitymapper.PropertyMetadata;
import com.oberasoftware.jasdb.api.entitymapper.TypeMapper;
import com.oberasoftware.jasdb.api.entitymapper.annotations.Id;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBEntity;
import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.oberasoftware.core.utils.AnnotationUtils.getAnnotation;
import static com.oberasoftware.core.utils.AnnotationUtils.getOptionalAnnotation;
import static com.oberasoftware.jasdb.entitymapper.types.TypeMapperFactory.getTypeMapper;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
@Component
public class AnnotationEntityMapper implements EntityMapper {
    private static final Logger LOG = getLogger(AnnotationEntityMapper.class);

    private ConcurrentMap<String, EntityMetadata> cachedEntityMetadata = new ConcurrentHashMap<>();

    @Override
    public MapResult mapTo(Object mappableObject) throws JasDBStorageException {
        LOG.debug("Mapping entity: {}", mappableObject);

        Class<?> entityClass = mappableObject.getClass();
        EntityMetadata metadata = getEntityMetadata(entityClass);
        Optional<PropertyMetadata> keyProperty = metadata.getKeyProperty();
        SimpleEntity entity = new SimpleEntity();

        if(keyProperty.isPresent()) {
            Object keyValue = EntityUtils.getValue(mappableObject, keyProperty.get());
            String id = getTypeMapper(String.class).mapToRawType(keyValue);

            if(EntityUtils.toValidUUID(id) != null) {
                LOG.debug("Setting entity id to: {}", id);
                entity.setInternalId(id);
            } else {
                LOG.warn("Entity marked with an @Id {} field, but invalid UUID, ignoring", id);
            }
        }

        metadata.getProperties().forEach((k, v) -> {
            try {
                Property property = EntityUtils.map(mappableObject, v);
                entity.addProperty(property);
            } catch (JasDBStorageException e) {
                throw new RuntimeJasDBException("Unable to map property: " + v, e);
            }
        });

        return new MapResultImpl(metadata, entity, mappableObject, metadata.getBagName());
    }

    @Override
    public <T> T mapFrom(Class<T> targetType, SimpleEntity entity) throws JasDBStorageException {
        try {
            Object instance = targetType.newInstance();
            EntityMetadata metadata = getEntityMetadata(targetType);

            metadata.getProperties().forEach((k, v) ->{
                Object value = v.getTypeMapper().mapFromProperty(entity.getProperty(k));

                try {
                    v.getWriteMethod().invoke(instance, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeJasDBException("Unable to set field value on entity: " + targetType, e);
                }
            });

            return targetType.cast(instance);
        } catch(IllegalAccessException | InstantiationException e) {
            throw new JasDBStorageException("Unable to create instance of entity, missing constructor or inaccessible: " + targetType, e);
        }
    }

    private EntityMetadata getEntityMetadata(Class<?> entityClass) throws JasDBStorageException {
        String entityClassName = entityClass.getName();

        if(!cachedEntityMetadata.containsKey(entityClassName)) {
            LOG.debug("Entity: {} metadata not cached, loading now", entityClassName);
            cachedEntityMetadata.putIfAbsent(entityClassName, loadEntityMetadata(entityClass));
        }
        return cachedEntityMetadata.get(entityClassName);
    }

    private EntityMetadata loadEntityMetadata(Class<?> entityClass) throws JasDBStorageException {
            JasDBEntity annotationEntity = getAnnotation(entityClass, JasDBEntity.class);
            String bagName = annotationEntity.bagName();

            Map<String, PropertyMetadata> propertyMetadatas = loadProperties(entityClass);
            Optional<PropertyMetadata> keyProperty = propertyMetadatas.values().stream().filter(PropertyMetadata::isKey).findFirst();
            LOG.debug("Found key property: {}", keyProperty);

            return new EntityMetadataImpl(entityClass, bagName, keyProperty, propertyMetadatas);
    }

    private Map<String, PropertyMetadata> loadProperties(Class<?> entityClass) throws JasDBStorageException {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(entityClass);
            Map<String, PropertyMetadata> propertyMetadatas = new HashMap<>();

            for(PropertyDescriptor p : beanInfo.getPropertyDescriptors()) {
                PropertyMetadata propertyMetadata = loadProperty(p);
                if(propertyMetadata != null) {
                    propertyMetadatas.put(propertyMetadata.getPropertyName(), propertyMetadata);
                }
            }

            LOG.debug("Mapped: {} properties to entity: {}", propertyMetadatas.size(), entityClass);

            return propertyMetadatas;
        } catch(IntrospectionException e) {
            throw new JasDBStorageException("Unable to map entity: " + entityClass);
        }
    }

    private PropertyMetadata loadProperty(PropertyDescriptor propertyDescriptor) throws JasDBStorageException {
        Method readMethod = propertyDescriptor.getReadMethod();
        Method writeMethod = propertyDescriptor.getWriteMethod();

        if(readMethod != null && writeMethod != null) {
            Optional<JasDBProperty> readAnnotation = getOptionalAnnotation(readMethod, JasDBProperty.class);
            Optional<JasDBProperty> writeAnnotation = getOptionalAnnotation(writeMethod, JasDBProperty.class);
            Optional<Id> idAnnotation = getOptionalAnnotation(readMethod, Id.class);

            if (readAnnotation.isPresent() || writeAnnotation.isPresent()) {
                TypeMapper typeMapper = getTypeMapper(readMethod);

                //here a number of override for the property name, first check read method, next write else default to property bean name
                String propertyName = readAnnotation.isPresent() ? readAnnotation.get().name() : "";
                propertyName = StringUtils.stringEmpty(propertyName) ? writeAnnotation.isPresent() ? writeAnnotation.get().name() : "" : propertyName;
                propertyName = StringUtils.stringEmpty(propertyName) ? propertyDescriptor.getName() : propertyName;

                LOG.debug("Found Entity property: {}", propertyDescriptor.getName());
                return new PropertyMetadataImpl(typeMapper, readMethod, writeMethod, propertyName, idAnnotation.isPresent());
            }
        } else {
            LOG.debug("Read or Write method not defined for property: {}", propertyDescriptor.getName());
        }

        return null;
    }

}
