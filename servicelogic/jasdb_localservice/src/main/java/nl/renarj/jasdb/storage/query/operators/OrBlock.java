/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.query.operators;

import nl.renarj.jasdb.storage.query.operators.mergers.OrBlockMerger;

public class OrBlock extends AbstractBlock {
	public OrBlock() {
		
	}

	@Override
	public BlockMerger getMerger() {
		return new OrBlockMerger();
	}
}
