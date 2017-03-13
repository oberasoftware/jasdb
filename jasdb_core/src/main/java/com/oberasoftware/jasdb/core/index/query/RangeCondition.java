/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.query;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

public class RangeCondition implements SearchCondition {
	private Key start;
	private boolean startIncluded;
	
	private Key end;
	private boolean endIncluded;
	
	public RangeCondition(Key start, Key end) {
		this.start = start;
		this.end = end;
		this.startIncluded = false;
		this.endIncluded = false;
	}
	
	public RangeCondition(Key start, boolean inclusiveStart, Key end, boolean inclusiveEnd) {
		this.start = start;
		this.end = end;
		this.startIncluded = inclusiveStart;
		this.endIncluded = inclusiveEnd;
	}
	
	@Override
	public boolean keyQualifies(Key key) {
        boolean startQualifies = true;
        if(start != null) {
            int c = key.compareTo(start);
            startQualifies = (startIncluded && c>=0) || c>0;
        }
        boolean endQualifies = true;
        if(end != null) {
            int c = key.compareTo(end);
            endQualifies = (endIncluded && c<=0) || c<0;
        }
        return startQualifies && endQualifies;
	}

	public boolean isStartIncluded() {
		return startIncluded;
	}

	public boolean isEndIncluded() {
		return endIncluded;
	}

	public Key getStart() {
		return this.start;
	}
	
	public void setStart(Key start) {
		this.start = start;
	}
	
	public Key getEnd() {
		return this.end;
	}
	
	public void setEnd(Key end) {
		this.end = end;
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

    @Override
    public SearchCondition mergeCondition(KeyNameMapper nameMapper, String sourceField, String mergeField, SearchCondition condition) {
        return null;
    }

    @Override
	public String toString() {
		StringBuilder conditionBuilder = new StringBuilder();
		conditionBuilder.append("Start key: ").append(start != null ? start : "");
		conditionBuilder.append(", Include start: ").append(start != null ? String.valueOf(startIncluded) : "");
		conditionBuilder.append(", End key: ").append(end != null ? end : "");
		conditionBuilder.append(", Include end: ").append(end != null ? String.valueOf(endIncluded) : "");
		
		return conditionBuilder.toString();
	}
}
