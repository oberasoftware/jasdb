package com.oberasoftware.jasdb.api.index.keys;

import java.util.Map;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public interface KeyNameMapper extends Cloneable {
    /**
     * Gets the index used for the given field
     * @param field The field to request the mapped index for
     * @return The mapped index
     */
    int getIndexForField(String field);

    /**
     * Provides a Map containing the index to field mappings
     * @return The index to field mappings
     */
    Map<Integer, String> getMappings();

    /**
     * Check if a field is mapped
     *
     * @param field The field to check if it is mapped
     * @return True if the field is mapped, False if not
     */
    boolean isMapped(String field);

    /**
     * The amount of mapped fields
     * @return THe amount of mapped fields
     */
    int size();

    /**
     * Gets the field for the index
     * @param index The index to get the field name for
     * @return THe field name if present, null if not
     */
    String getFieldForIndex(Integer index);

    /**
     * Gets a set of fields mapper
     * @return The set of mapped fields
     */
    Set<String> getFieldSet();

    /**
     * Clones the key name mapper
     * @return The key name mapper
     */
    KeyNameMapper clone();

    void setValueMarker(int index);

    int getValueMarker();

    Integer addMappedField(String field);

    boolean isFullyMapped(Key key);
}
