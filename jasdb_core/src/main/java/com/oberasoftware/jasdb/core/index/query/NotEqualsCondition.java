package com.oberasoftware.jasdb.core.index.query;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;

/**
 * @author Renze de Vries
 */
public class NotEqualsCondition extends EqualsCondition {
    public NotEqualsCondition(Key key) {
        super(key);
    }

    @Override
    public boolean keyQualifies(Key key) {
        return getKey().compareTo(key) != 0;
    }

    @Override
    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition) {
        if(condition instanceof NotEqualsCondition) {
            CompositeKey compositeKey;
            Key key = getKey();
            if(key instanceof CompositeKey) {
                compositeKey = (CompositeKey) key;
            } else {
                compositeKey = new CompositeKey();
                compositeKey.addKey(nameMapper, sourceField, key);
            }
            return new NotEqualsCondition(compositeKey.addKey(nameMapper, mergeField, ((EqualsCondition) condition).getKey()));
        }

        return null;
    }

}
