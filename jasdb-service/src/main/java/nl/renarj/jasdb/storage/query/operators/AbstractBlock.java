/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 *
 * All the code and design principals in the codebase are also Copyright 2011
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.query.operators;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractBlock implements BlockOperation {
	private Map<String, Set<SearchCondition>> conditions;
	private Set<BlockOperation> blockOperations;

	protected AbstractBlock() {
		conditions = new HashMap<>();
		blockOperations = new HashSet<>();
	}

	public void addChildBlock(BlockOperation operation) {
		this.blockOperations.add(operation);
	}

    @Override
    public boolean isEmpty() {
        return conditions.isEmpty() && blockOperations.isEmpty();
    }

    @Override
    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition) {
        return this;
    }

    @Override
	public Set<BlockOperation> getChildBlocks() {
		return this.blockOperations;
	}

	@Override
	public boolean keyQualifies(Key key) {
		return false;
	}

	public void addCondition(String field, SearchCondition condition) {
		Set<SearchCondition> fieldConditions = null;
		if(!this.conditions.containsKey(field)) {
			fieldConditions = new HashSet<>();
			
			this.conditions.put(field, fieldConditions);
		} else {
			fieldConditions = this.conditions.get(field);
		}
		
		fieldConditions.add(condition);
	}
	
	@Override
	public Map<String, Set<SearchCondition>> getConditions() {
		return Collections.unmodifiableMap(this.conditions);		
	}

    @Override
    public boolean hasConditions(String field) {
        return this.conditions.containsKey(field);
    }

    @Override
	public Set<SearchCondition> getConditions(String field) {
		if (this.conditions.containsKey(field)) {
			return this.conditions.get(field);
		} else {
			return Collections.emptySet();
		}
	}

    @Override
    public Set<SearchCondition> getConditions(KeyNameMapper mapper, List<String> fields) {
        Set<SearchCondition> currentConditions = new HashSet<>();
        if(fields.size() == 1) {
            currentConditions = getConditions(fields.get(0));
        } else if(fields.size() > 1) {
            String primaryField = null;
            for(String field : fields) {
                Set<SearchCondition> fieldConditions = getConditions(field);
                if(!fieldConditions.isEmpty()) {
                    if(currentConditions.isEmpty()) {
                        currentConditions = fieldConditions;
                        primaryField = field;
                    } else {
                        currentConditions = mergeConditions(mapper, primaryField, currentConditions, field, fieldConditions);
                    }
                }
            }
        }
        return currentConditions;
    }

    private Set<SearchCondition> mergeConditions(KeyNameMapper nameMapper, String field1, Set<SearchCondition> conditionSet1, String field2, Set<SearchCondition> conditionSet2) {
        Set<SearchCondition> mergedConditions = new HashSet<>();
        for(SearchCondition condition1 : conditionSet1) {
            for(SearchCondition condition2 : conditionSet2) {
                mergedConditions.add(condition1.mergeCondition(nameMapper, field1, field2, condition2));
            }
        }
        return mergedConditions;
    }

    @Override
	public Set<String> getFields() {
		return Collections.unmodifiableSet(conditions.keySet());
	}
}
