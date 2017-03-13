/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */

package com.oberasoftware.jasdb.core;

import com.oberasoftware.jasdb.api.caching.CachableItem;
import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.*;
import com.oberasoftware.jasdb.core.serializer.EntityDeserializer;
import com.oberasoftware.jasdb.core.serializer.EntitySerializer;
import com.oberasoftware.jasdb.core.serializer.json.JsonEntityDeserializer;
import com.oberasoftware.jasdb.core.serializer.json.JsonEntitySerializer;

import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

/**
 * This is the public API storage entity, this is stored in the DB and all indexes are
 * updated based on the index definitions. The Entity allows adding of properties and nested
 * entities which will automatically be serialized into the internal storage format.
 * 
 * @author Renze de Vries
 *
 */
public class SimpleEntity implements Serializable, CachableItem, Entity {
    private static final EntitySerializer serializer = new JsonEntitySerializer();
    private static final EntityDeserializer deserializer = new JsonEntityDeserializer();


	private static final long serialVersionUID = 4323161274218796585L;



	private String internalId;
	private Map<String, Property> properties;

    public SimpleEntity(String internalId) {
		properties = new TreeMap<>();
		setInternalId(internalId);
	}
	
	public SimpleEntity() {
		this(null);
	}
	
	@Override
    public String getInternalId() {
		return internalId;
	}
	
	@Override
    public void setInternalId(String internalId) {
		this.internalId = internalId;
        if(properties.containsKey(DOCUMENT_ID)) {
            removeProperty(DOCUMENT_ID);
        }
        addValueToProperty(DOCUMENT_ID, new StringValue(internalId));
	}

    /**
     * This allows directly adding a constructed Property object to the entity. If any existing properties
     * exist with the same name these are overriden.
     *
     * @param property The property object
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(Property property) {
        properties.put(property.getPropertyName(), property);
        return this;
    }

    /**
     * Adds a property with a String value
     * @param propertyName The name of the property
     * @param stringValue The string value
     * @return The entity the property has been added to
     */
	@Override
    public SimpleEntity addProperty(String propertyName, String stringValue) {
        addValueToProperty(propertyName, new StringValue(stringValue));
        return this;
	}

    /**
     * Adds a property with a String value
     * @param propertyName The name of the property
     * @param isCollection True if this is a collection, False if not
     * @param stringValue The string value
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, boolean isCollection, String stringValue) {
        addValueToProperty(propertyName, isCollection, new StringValue(stringValue));
        return this;
    }

    /**
     * Adds a property with a colleciton of String values
     * @param propertyName The name of the property
     * @param stringValues The string values to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, String... stringValues) {
        for(String value : stringValues) {
            addValueToProperty(propertyName, new StringValue(value));
        }
        return this;
    }

    /**
     * Adds a property with a list of String values
     * @param propertyName The name of the property
     * @param stringValues The list of string values to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, List<String> stringValues) {
        for(String value : stringValues) {
            addValueToProperty(propertyName, true, new StringValue(value));
        }
        return this;
    }

    /**
     * Adds a property with an Integer value
     * @param propertyName The name of the property
     * @param intValue The integer value to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, int intValue) {
        addValueToProperty(propertyName, new IntegerValue(intValue));
        return this;
	}

    /**
     * Adds a property with an Integer value
     * @param propertyName The name of the property
     * @param isCollection True if this is a collection, False if not
     * @param intValue The integer value to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, boolean isCollection, int intValue) {
        addValueToProperty(propertyName, isCollection, new IntegerValue(intValue));
        return this;
    }

    /**
     * Adds a property with a Long value
     * @param propertyName The name of the property
     * @param longValue The long value to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, long longValue) {
        addValueToProperty(propertyName, new LongValue(longValue));
        return this;
	}

    /**
     * Adds a property with a Long value
     * @param propertyName The name of the property
     * @param isCollection True if this is a collection, False if not
     * @param longValue The long value to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, boolean isCollection, long longValue) {
        addValueToProperty(propertyName, isCollection, new LongValue(longValue));
        return this;
    }

    /**
     * Adds a property with boolean value
     * @param propertyName the name of the property
     * @param booleanValue True or False boolean value
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, boolean booleanValue) {
        addValueToProperty(propertyName, new BooleanValue(booleanValue));
        return this;
    }

    /**
     * Adds a property with a collection of long values
     * @param propertyName The name of the property
     * @param longValues The collection of long values to add to the property
     * @return The entity the property has been added to
     */
    @Override
    public SimpleEntity addProperty(String propertyName, long... longValues) {
        for(long longValue : longValues) {
            addValueToProperty(propertyName, new LongValue(longValue));
        }
        return this;
    }

    /**
     * Sets a property with given name to the current value, overriding any current
     * value and property set for that given property name
     * @param propertyName THe name of the property
     * @param longValue The long value to add or override current values
     * @return The entity the property has been set on
     */
    @Override
    public SimpleEntity setProperty(String propertyName, long longValue) {
        removeProperty(propertyName);
        addValueToProperty(propertyName, new LongValue(longValue));
        return this;
    }

    /**
     * Sets a property with given name to the current value, overriding any current
     * value and property set for that given property name
     * @param propertyName THe name of the property
     * @param longValues The long values to add or override current values
     * @return The entity the property has been set on
     */
    @Override
    public SimpleEntity setProperty(String propertyName, long... longValues) {
        removeProperty(propertyName);
        addProperty(propertyName, longValues);
        return this;
    }

    /**
     * Sets a property with given name to the current value, overriding any current
     * value and property set for that given property name.
     * @param propertyName THe name of the property
     * @param values The string values to add or override current values
     * @return The entity the property has been set on
     */
    @Override
    public SimpleEntity setProperty(String propertyName, String... values) {
        removeProperty(propertyName);
        addProperty(propertyName, values);
        return this;
    }

    /**
     * Sets a property with given name to the current value, overriding any current
     * value and property set for that given property name
     * @param propertyName THe name of the property
     * @param value The Integer value to add or override current values
     * @return The entity the property has been set on
     */
    @Override
    public SimpleEntity setProperty(String propertyName, int value) {
        removeProperty(propertyName);
        addValueToProperty(propertyName, new IntegerValue(value));
        return this;
    }

    /**
     * Allows a bulk set operation of the properties. The properties map contains a key
     * value pair, the values can be of type {@link java.lang.Integer} , {@link java.lang.Long}
     * or {@link java.lang.String}. Any unknown types will be called by using the toString operation.
     *
     * @param newProperties The properties containing key value pairs
     * @return The entity the properties have been set on
     */
    @Override
    public SimpleEntity setProperties(Map<String, Object> newProperties) {
        this.properties.clear();
        for(Map.Entry<String, Object> entry : newProperties.entrySet()) {
            if(entry.getValue() instanceof Integer) {
                addValueToProperty(entry.getKey(), new IntegerValue((Integer) entry.getValue()));
            } else if(entry.getValue() instanceof Long) {
                addValueToProperty(entry.getKey(), new LongValue((Long) entry.getValue()));
            } else {
                addValueToProperty(entry.getKey(), new StringValue(entry.getValue().toString()));
            }
        }
        return this;
    }

    /**
     * Creates an empty property object, the proeprty object can be used
     * to set values on, if no values are set the property object is not
     * serialized
     * @param propertyName The property name
     * @return The property created for the given property name
     */
    @Override
    public Property createProperty(String propertyName) {
        if(!properties.containsKey(propertyName)) {
            Property property = new MultivalueProperty(propertyName);
            properties.put(propertyName, property);
            return property;
        } else {
            return properties.get(propertyName);
        }
    }

    /**
     * Removes the given property from the entity
     * @param propertyName The name of the property to remove
     * @return The entity the property was removed on
     */
    @Override
    public SimpleEntity removeProperty(String propertyName) {
        properties.remove(propertyName);
        return this;
    }

    /**
     * Adds an embedded entity to the current entity with the given property name
     * @param propertyName The name to add the embedded entity on
     * @param entity The entity to embed
     * @return The entity onto which the entity was embedded
     */
	@Override
    public SimpleEntity addEntity(String propertyName, Entity entity) {
        addValueToProperty(propertyName, new EntityValue(entity));
        return this;
	}

    private void addValueToProperty(String propertyName, Value value) {
	    addValueToProperty(propertyName, false, value);
    }
    
    private void addValueToProperty(String propertyName, boolean isCollection, Value value) {
        Property mvProperty;
        if(properties.containsKey(propertyName)) {
            mvProperty = properties.get(propertyName);
        } else {
            mvProperty = new MultivalueProperty(propertyName, isCollection);
            properties.put(propertyName, mvProperty);
        }
        mvProperty.addValue(value);
    }

    /**
     * Returns whether a property exist on the current entity
     * @param propertyName The name of the property to check for existence
     * @return True if the property exists, False if not
     */
	@Override
    public boolean hasProperty(String propertyName) {
		return getProperty(propertyName) != null;
	}

    @Override
    public boolean hasEntity(String entityName) {
        return properties.containsKey(entityName) && properties.get(entityName).getFirstValueObject() instanceof EmbeddedEntity;
    }

    @Override
    public boolean hasValue(String propertyName) {
        return hasProperty(propertyName);
    }

    @Override
    public List<Property> getProperties() {
		return new ArrayList<>(properties.values());
	}

    /**
     * Gets a property object for the given property name
     * @param propertyName The name of the property
     * @return The property if present, Null if no property present for given name
     */
	@Override
    public Property getProperty(String propertyName) {
        if(propertyName.contains(".")) {
            String[] pathElements = propertyName.split("\\.");

            SimpleEntity currentEntity = this;
            for(String pathElement : pathElements) {
                if(currentEntity.hasEntity(pathElement)) {
                    currentEntity = currentEntity.getEntity(pathElement);
                } else if(currentEntity.hasProperty(pathElement)) {
                    return currentEntity.getProperty(pathElement);
                }
            }
        }

		if(properties.containsKey(propertyName)) {
			return properties.get(propertyName);
		}
		
		return null;
	}

    @Override
    public SimpleEntity getEntity(String embeddedEntityName) {
        Object value = getValue(embeddedEntityName);
        if(value != null && value instanceof EmbeddedEntity) {
            return (EmbeddedEntity) value;
        }
        return null;
    }
	
	@Override
	public <T> T getValue(String propertyName) {
        Property property = getProperty(propertyName);
		if(property != null) {
			return property.getFirstValueObject();
		}
		return null;
	}

    @Override
    public <T> List<T> getValues(String propertyName) {
        Property property = getProperty(propertyName);
        if(property != null) {
            return property.getValueObjects();
        }
        return null;
    }

    @Override
    public boolean isMultiValue(String propertyName) {
        return properties.containsKey(propertyName) && properties.get(propertyName).isMultiValue();
    }

    @Override
	public int hashCode() {
		return this.internalId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SimpleEntity) {
			SimpleEntity entity = (SimpleEntity) obj;
			if(entity.getInternalId() != null && entity.getInternalId().equals(this.internalId)) {
				return true;
			}
		} 
		
		return false;
	}

    /**
     * Deserialize the entity from json structure into the SimpleEntity model.
     * @param recordResult The string containing an entity as json
     * @return The deserialized SimpleEntity from the json
     * @throws MetadataParseException If unable to deserialize the json into the SimpleEntity
     */
	public static Entity fromJson(String recordResult) throws MetadataParseException {
        return deserializer.deserializeEntity(recordResult);
	}

    /**
     * Deserialize the entity from json inputstream into the SimpleEntity model.
     * @param inputStream The inputstream containing json data
     * @return The deserialized SimpleEntity from the json
     * @throws MetadataParseException If unable to deserialize the json into the SimpleEntity
     */
    public static Entity fromStream(InputStream inputStream) throws MetadataParseException {
        return deserializer.deserializeEntity(inputStream);
    }

    /**
     * Serialize the entity into json format
     * @param entity The entity to serialize into json
     * @return The serialize string containing the entity as json
     * @throws MetadataParseException If unable to serialize the entity
     */
	public static String toJson(Entity entity) throws MetadataParseException {
        return serializer.serializeEntity(entity);
	}

	@Override
	public long getObjectSize() {
        return 0;
	}

    @Override
    public SimpleEntity clone() {
        SimpleEntity clone = new SimpleEntity(internalId);
        clone.properties = new HashMap<>(properties);
        return clone;
    }

    @Override
    public String toString() {
        return "SimpleEntity{" +
                "internalId='" + internalId + '\'' +
                '}';
    }
}
