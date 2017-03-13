/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session.query;

import java.util.UUID;


public final class QueryField {
	private String field;
	private Object searchKey;
	private QueryFieldOperator operator;

	private QueryField(String field, Object searchKey, QueryFieldOperator operator) {
		this.field = field;
		this.operator = operator;
		this.searchKey = searchKey;
	}	
	
	public QueryField(String field, String value) {
		this(field, value, QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, UUID value) {
		this(field, value, QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, long value) {
		this(field, value, QueryFieldOperator.EQUALS);
	}
	
	public QueryField(String field, String value, QueryFieldOperator operator) {
		this(field, (Object)value, operator);
	}
	
	public QueryField(String field, long value, QueryFieldOperator operator) {
		this(field, (Object)value, operator);
	}

	public QueryField(String field, UUID value, QueryFieldOperator operator) {
		this(field, (Object)value, operator);
	}

	public String getField() {
		return field;
	}

    public Object getSearchKey() {
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