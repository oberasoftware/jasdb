/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine.query.operators;

import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BlockOperation extends SearchCondition {
	BlockMerger getMerger();

	boolean isEmpty();
	
	void addCondition(String field, SearchCondition condition);
	void addChildBlock(BlockOperation operation);
	Map<String, Set<SearchCondition>> getConditions();
	Set<String> getFields();
    boolean hasConditions(String field);
    Set<SearchCondition> getConditions(KeyNameMapper mapper, List<String> fields);
	Set<SearchCondition> getConditions(String field);

	Set<BlockOperation> getChildBlocks();
}
