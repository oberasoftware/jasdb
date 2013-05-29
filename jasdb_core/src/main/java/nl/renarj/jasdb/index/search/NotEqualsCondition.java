package nl.renarj.jasdb.index.search;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

/**
 * @author Renze de Vries
 */
public class NotEqualsCondition extends EqualsCondition {
    public NotEqualsCondition(Key key) {
        super(key);
    }

    @Override
    public boolean keyQualifies(Key key) {
        return this.key.compareTo(key) != 0;
    }

    @Override
    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition) {
        if(condition instanceof NotEqualsCondition) {
            CompositeKey compositeKey;
            if(key instanceof CompositeKey) {
                compositeKey = (CompositeKey) key;
            } else {
                compositeKey = new CompositeKey();
                compositeKey.addKey(nameMapper, sourceField, key);
            }
            return new NotEqualsCondition(compositeKey.addKey(nameMapper, mergeField, ((EqualsCondition)condition).getKey()));
        }

        return null;
    }

}
