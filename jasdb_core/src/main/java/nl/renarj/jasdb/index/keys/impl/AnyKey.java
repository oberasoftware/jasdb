package nl.renarj.jasdb.index.keys.impl;

import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.index.keys.AbstractKey;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.CompareResult;
import nl.renarj.jasdb.index.keys.Key;

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
