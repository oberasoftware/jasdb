package com.oberasoftware.jasdb.api.index;

import com.oberasoftware.jasdb.api.index.keys.KeyType;

/**
 * @author renarj
 */
public interface IndexField {
    /**
     * Gets the field that needs to be indexed
     * @return The field name to be index
     */
    String getField();

    /**
     * Gets the key type
     * @return The key type
     */
    KeyType getKeyType();
}
