/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session.query;

public class FieldBuilder {
	private QueryBuilder builder;
	private String field;
	
	protected FieldBuilder(String field, QueryBuilder builder) {
		this.builder = builder;
		this.field = field;
	}
	
	public QueryBuilder value(String value) {
		builder.addQueryField(new QueryField(field, value));
		return this.builder;
	}
	
	public QueryBuilder value(long value) {
		builder.addQueryField(new QueryField(field, value));
		return this.builder;
	}
	
	public QueryBuilder greaterThan(String value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.LARGER_THAN));
		return this.builder;
	}
	
	public QueryBuilder greaterThan(long value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.LARGER_THAN));
		return this.builder;
	}
	
	public QueryBuilder greaterThanOrEquals(String value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.LARGER_THAN_OR_EQUALS));
		return this.builder;
	}
	
	public QueryBuilder greaterThanOrEquals(long value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.LARGER_THAN_OR_EQUALS));
		return this.builder;
	}
	
	public QueryBuilder smallerThan(String value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.SMALLER_THAN));
		return this.builder;
	}
	
	public QueryBuilder smallerThan(long value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.SMALLER_THAN));
		return this.builder;
	}
	
	public QueryBuilder smallerThanOrEquals(String value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.SMALLER_THAN_OR_EQUALS));
		return this.builder;
	}
	
	public QueryBuilder smallerThanOrEquals(long value) {
		builder.addQueryField(new QueryField(field, value, QueryFieldOperator.SMALLER_THAN_OR_EQUALS));
		return this.builder;
	}

    public QueryBuilder notEquals(String value) {
        builder.addQueryField(new QueryField(field, value, QueryFieldOperator.NOT_EQUALS));
        return this.builder;
    }

    public QueryBuilder notEquals(long value) {
        builder.addQueryField(new QueryField(field, value, QueryFieldOperator.NOT_EQUALS));
        return this.builder;
    }
	
	public QueryBuilder operation(QueryFieldOperator operator, String value) {
		builder.addQueryField(new QueryField(field, value, operator));
		return this.builder;
	}
	
	public QueryBuilder operation(QueryFieldOperator operator, long value) {
		builder.addQueryField(new QueryField(field, value, operator));
		return this.builder;
	}
}
