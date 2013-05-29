/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.query;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;

import java.util.UUID;


public final class QueryField {
	private String field;
	private Key searchKey;
	private QueryFieldOperator operator;

	private QueryField(String field, Key searchKey, QueryFieldOperator operator) {
		this.field = field;
		this.operator = operator;
		this.searchKey = searchKey;
	}	
	
	public QueryField(String field, String value) {
		this(field, new StringKey(value), QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, UUID value) {
		this(field, new UUIDKey(value), QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, long value) {
		this(field, new LongKey(value), QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, String value, QueryFieldOperator operator) {
		this(field, new StringKey(value), operator);
	}
	
	public QueryField(String field, long value, QueryFieldOperator operator) {
		this(field, new LongKey(value), operator);
	}

	public QueryField(String field, UUID value, QueryFieldOperator operator) {
		this(field, new UUIDKey(value), operator);
	}


	public String getField() {
		return field;
	}

    public Key getSearchKey() {
        return searchKey;
    }

    public QueryFieldOperator getOperator() {
        return operator;
    }

    @Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryField) {
			return obj.hashCode() == hashCode();
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Field: ").append(field).append(";");
		builder.append("Operator: ").append(operator).append(";");
		builder.append("Value: ").append(searchKey);
		
		return builder.toString();
	}
	
	
}