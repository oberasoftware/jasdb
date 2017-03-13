package com.oberasoftware.jasdb.core.index.keys.types;

import com.oberasoftware.jasdb.api.index.keys.KeyType;

/**
 * @author Renze de Vries
 */
public class ComplexKeyType implements KeyType {
    public static final String KEY_ID = "complexType";

    @Override
    public String getKeyId() {
        return KEY_ID;
    }

    @Override
    public String[] getKeyArguments() {
        return new String[0];
    }
}
