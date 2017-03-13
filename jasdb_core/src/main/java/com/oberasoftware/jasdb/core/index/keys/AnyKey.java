package com.oberasoftware.jasdb.core.index.keys;

import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.core.index.keys.AbstractKey;
import com.oberasoftware.jasdb.api.index.keys.CompareMethod;
import com.oberasoftware.jasdb.api.index.keys.CompareResult;
import com.oberasoftware.jasdb.api.index.keys.Key;

/**
 * @author renarj
 */
public class AnyKey extends AbstractKey {

    private static final int ALWAYS_EQUALS = 0;

    @Override
    public Key cloneKey() {
        throw new RuntimeJasDBException("Not implemented");
    }

    @Override
    public Key cloneKey(boolean includeChildren) {
        throw new RuntimeJasDBException("Not implemented");
    }

    @Override
    public Object getValue() {
        throw new RuntimeJasDBException("Not implemented");
    }

    @Override
    public CompareResult compare(Key otherKey, CompareMethod method) {
        return new CompareResult(ALWAYS_EQUALS);
    }
}
