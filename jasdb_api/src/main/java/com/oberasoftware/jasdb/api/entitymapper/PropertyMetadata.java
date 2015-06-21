package com.oberasoftware.jasdb.api.entitymapper;

import java.lang.reflect.Method;

/**
 * @author Renze de Vries
 */
public interface PropertyMetadata {
    TypeMapper getTypeMapper();

    String getPropertyName();

    Method getReadMethod();

    Method getWriteMethod();

    boolean isKey();
}
