/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine.query.operators;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;

public interface BlockMerger {
	public IndexSearchResultIteratorCollection mergeIterators(IndexSearchResultIteratorCollection mergeInto, IndexSearchResultIteratorCollection... results) throws JasDBStorageException;
	
	public boolean includeResult(boolean leftResultFound, boolean rightResultFound);

    public boolean continueEvaluation(boolean currentState);
}
