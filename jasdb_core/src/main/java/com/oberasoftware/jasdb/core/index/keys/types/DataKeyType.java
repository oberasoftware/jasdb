package com.oberasoftware.jasdb.core.index.keys.types;

import com.oberasoftware.jasdb.api.index.keys.KeyType;

/**
 * @author Renze de Vries
 */
public class DataKeyType implements KeyType {
    public static final String KEY_ID = "dataType";

    @Override
    public String getKeyId() {
        return KEY_ID;
    }

    @Override
    public String[] getKeyArguments() {
        return new String[] {};
    }
}
