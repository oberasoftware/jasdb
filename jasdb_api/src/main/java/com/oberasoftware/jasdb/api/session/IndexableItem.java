package com.oberasoftware.jasdb.api.session;

import java.util.List;

public interface IndexableItem {
    /**
     * Returns whether a property has a value
     * @param propertyName The name of the property to check for a value
     * @return True if the property has a value, False if not
     */
    boolean hasValue(String propertyName);

    /**
     * Gets the first value of the property, in case of multivalue
     * picks the first value in the collection.
     * @param propertyName The name of the property to retrieve the value for
     * @return The first value for the given property
     */
	<T> T getValue(String propertyName);

    /**
     * Gets a collection of objects representing the values of a given
     * property
     * @param propertyName The name of the property to get the values for
     * @return The collection of property values
     */
    <T> List<T> getValues(String propertyName);

    /**
     * Returns wether a property is multivalue or not
     * @param propertyName THe name of the property to check for multivalue
     * @return True if the property is multivalue, False if not
     */
    boolean isMultiValue(String propertyName);
}
