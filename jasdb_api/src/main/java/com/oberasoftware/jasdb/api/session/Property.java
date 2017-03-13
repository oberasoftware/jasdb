/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session;

import java.util.List;

/**
 * This represent a property containing a value or collection of values for an entity
 *
 * @author Renze de Vries
 */
public interface Property {
    /**
     * Gets the name of the property
     * @return The name of the property
     */
	String getPropertyName();

    /**
     * Creates a serialized string of the values of this property
     * @return The serialized string values
     */
	String toString();

    /**
     * Gets the first value of this property as an object, can be of
     * any value type (String, Integer, Long, etc.)
     * @return The value object
     */
    <T> T getFirstValueObject();

    /**
     * Gets the Value object wrapper of the first value
     * @return The value object wrapper of the first value
     */
	Value getFirstValue();

    /**
     * Gets a list of all the value wrappers
     * @return The list of all the values
     */
    List<Value> getValues();

    /**
     * Gets a list of all the value objects
     * @return the list of all the value objects
     */
    <T> List<T> getValueObjects();

    /**
     * Returns if this property has multiple values
     * @return True if this property has multiple values, False if not
     */
    boolean isMultiValue();

    /**
     * Returns if this property has values
     * @return True if this property has values
     */
    boolean hasValues();

    /**
     * Adds a value wrapper to this property
     * @param value The value wrapper
     * @return The property the value was added to
     */
    Property addValue(Value value);

    /**
     * Removes a value from the property
     * @param value The value to remove
     * @return The property the value was removed from
     */
    Property removeValue(Value value);
}
