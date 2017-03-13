package com.oberasoftware.jasdb.api.session;

import java.util.List;
import java.util.Map;

/**
 * @author renarj
 */
public interface Entity extends IndexableItem {
    String DOCUMENT_ID = "__ID";

    String getInternalId();

    void setInternalId(String internalId);

    Entity addProperty(Property property);

    Entity addProperty(String propertyName, String stringValue);

    Entity addProperty(String propertyName, boolean isCollection, String stringValue);

    Entity addProperty(String propertyName, String... stringValues);

    Entity addProperty(String propertyName, List<String> stringValues);

    Entity addProperty(String propertyName, int intValue);

    Entity addProperty(String propertyName, boolean isCollection, int intValue);

    Entity addProperty(String propertyName, long longValue);

    Entity addProperty(String propertyName, boolean isCollection, long longValue);

    Entity addProperty(String propertyName, boolean booleanValue);

    Entity addProperty(String propertyName, long... longValues);

    Entity setProperty(String propertyName, long longValue);

    Entity setProperty(String propertyName, long... longValues);

    Entity setProperty(String propertyName, String... values);

    Entity setProperty(String propertyName, int value);

    Entity setProperties(Map<String, Object> newProperties);

    Property createProperty(String propertyName);

    Entity removeProperty(String propertyName);

    Entity addEntity(String propertyName, Entity entity);

    boolean hasProperty(String propertyName);

    boolean hasEntity(String entityName);

    List<Property> getProperties();

    Property getProperty(String propertyName);

    Entity getEntity(String embeddedEntityName);
}
