/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.factory;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: renarj
 * Date: 2/13/12
 * Time: 9:37 PM
 */
public abstract class AbstractKeyFactory implements KeyFactory {
    protected String field;
    
    protected AbstractKeyFactory(String field) {
        this.field = field;
    }

    @Override
    public Set<Key> createMultivalueKeys(IndexableItem indexableItem) throws JasDBStorageException {
        List<Object> values = indexableItem.getValues(field);
        Set<Key> keys = new HashSet<>(values.size());
        for(Object value : values) {
            keys.add(convertToKey(value));
        }
        return keys;
    }

    @Override
    public boolean isMultiValueKey(IndexableItem indexableItem) throws JasDBStorageException {
        return indexableItem.isMultiValue(field);
    }

    protected abstract Key convertToKey(Object value) throws JasDBStorageException;
}
