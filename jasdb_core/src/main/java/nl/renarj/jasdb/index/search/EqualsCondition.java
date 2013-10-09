/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.search;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

public class EqualsCondition implements SearchCondition {
	protected Key key;
	
	public EqualsCondition(Key key) {
		this.key = key;
	}
	
	public Key getKey() {
		return this.key;
	}
	
	@Override
	public boolean keyQualifies(Key key) {
		return this.key.compareTo(key) == 0;
	}

    @Override
    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition) {
        if(condition instanceof EqualsCondition) {
            CompositeKey compositeKey;
            if(key instanceof CompositeKey) {
                compositeKey = (CompositeKey) key;
            } else {
                compositeKey = new CompositeKey();
                compositeKey.addKey(nameMapper, sourceField, key);
            }
            return new EqualsCondition(compositeKey.addKey(nameMapper, mergeField, ((EqualsCondition)condition).getKey()));
        } else if(condition instanceof RangeCondition) {
            RangeCondition rangeCondition = (RangeCondition) condition;

            Key startKey = null, endKey = null;
            if(rangeCondition.getStart() != null) {
                startKey = new CompositeKey().addKey(nameMapper, sourceField, key).addKey(nameMapper, mergeField, rangeCondition.getStart());
            }
            if(rangeCondition.getEnd() != null) {
                endKey = new CompositeKey().addKey(nameMapper, sourceField, key).addKey(nameMapper, mergeField, rangeCondition.getEnd());
            }
            return new RangeCondition(startKey, rangeCondition.isStartIncluded(), endKey, rangeCondition.isEndIncluded());
        }

        return null;
    }

    @Override
	public String toString() {
		StringBuilder conditionBuilder = new StringBuilder();
		conditionBuilder.append("Equals: ").append(key);
		
		return conditionBuilder.toString();
	}
}
