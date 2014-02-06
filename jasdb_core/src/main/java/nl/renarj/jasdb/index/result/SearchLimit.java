/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.result;

public class SearchLimit {
	public static final int UNLIMITED_RESULTS = -1;
	public static final int BEGIN = 0; 
	
	private int begin;
	private int max;
	
	public SearchLimit(int begin, int max) {
		this.begin = begin;
        this.max = max;
	}
	
	public SearchLimit(int max) {
		this.begin = BEGIN;
		this.max = max;
	}
	
	public SearchLimit() {
		this.begin = BEGIN;
		this.max = UNLIMITED_RESULTS;
	}
    
	public int getBegin() {
		return begin;
	}
	
	public int getMax() {
		return max;
	}

	public boolean isMaxReached(int currentAmount) {
        int lim = max;
        if(begin > 0) {
            lim = begin + max;
        }

        return !((currentAmount < lim) || (lim == UNLIMITED_RESULTS));
	}
}
