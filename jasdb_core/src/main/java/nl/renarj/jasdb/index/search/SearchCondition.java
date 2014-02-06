/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.search;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

public interface SearchCondition {
	public boolean keyQualifies(Key key);

    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition);
}
