package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class MockIndexableItem implements IndexableItem {
    private String field;
    private long value;
    private long recordPointer;

    public MockIndexableItem(String field, long value, long recordPointer) {
        this.field = field;
        this.value = value;
        this.recordPointer = recordPointer;
    }

    @Override
    public boolean hasValue(String propertyName) {
        return propertyName.equals(field) || propertyName.equals(KeyUtil.RECORD_POINTER);
    }

    @Override
    public Object getValue(String propertyName) {
        if(propertyName.equals(field)) {
            return value;
        } else {
            return recordPointer;
        }
    }

    @Override
    public List<Object> getValues(String propertyName) {
        return null;
    }

    @Override
    public boolean isMultiValue(String propertyName) {
        return false;
    }
}
