/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.query.operators;

import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BlockOperation extends SearchCondition {
	public BlockMerger getMerger();
	
	public void addCondition(String field, SearchCondition condition);
	public void addChildBlock(BlockOperation operation);
	public Map<String, Set<SearchCondition>> getConditions();
	public Set<String> getFields();
    public boolean hasConditions(String field);
    public Set<SearchCondition> getConditions(KeyNameMapper mapper, List<String> fields);
	public Set<SearchCondition> getConditions(String field);

	public Set<BlockOperation> getChildBlocks();
}
