/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryBuilder {
	private Map<String, List<QueryField>> queryFields;
	private BlockType type;
	
	private List<QueryBuilder> blocks;
	private List<SortParameter> sortParams;
	
	public QueryBuilder() {
		this(BlockType.AND);
	}
	
	public QueryBuilder(BlockType type) {
		this.type = type;
		queryFields = new HashMap<>();
		blocks = new ArrayList<>();
		sortParams = new ArrayList<>();
	}
	
	public static QueryBuilder createBuilder() {
		return new QueryBuilder();
	}
	
	public static QueryBuilder createBuilder(BlockType type) {
		return new QueryBuilder(type);
	}
	
	public FieldBuilder field(String fieldName) {
		return new FieldBuilder(fieldName, this);
	}
	
	public QueryBuilder field(QueryField field) {
		addQueryField(field);
		return this;
	}
	
	public QueryBuilder sortBy(String field, Order order) {
		this.sortParams.add(new SortParameter(field, order));
		return this;
	}

	public QueryBuilder sortBy(String field) {
		sortBy(field, Order.ASCENDING);
		return this;
	}
	
	public QueryBuilder sortBy(SortParameter sortParam) {
		this.sortParams.add(sortParam);
		return this;
	}
	
	public QueryBuilder and(QueryBuilder queryBuilder) {
        this.addQueryBlock(queryBuilder);
		return this;
	}
	
	public QueryBuilder or(QueryBuilder queryBuilder) {
		QueryBuilder parentBuilder = new QueryBuilder(BlockType.OR);
		
		parentBuilder.addQueryBlock(this);
		parentBuilder.addQueryBlock(queryBuilder);
		
		return parentBuilder;
	}
	
	protected void addQueryField(QueryField queryField) {
		List<QueryField> queryFields = createOrGetFields(queryField.getField());
		if(!queryFields.contains(queryField)) {
			queryFields.add(queryField);
		}
	}
	
	public void addQueryBlock(QueryBuilder builder) {
		this.blocks.add(builder);
	}
	
	public BlockType getBlockType() {
		return this.type;
	}
	
	public List<QueryBuilder> getChildBuilders() {
		return this.blocks;
	}
	
	public List<SortParameter> getSortParams() {
		return this.sortParams;
	}
	
	public Map<String, List<QueryField>> getQueryFields() {
		return this.queryFields;
	}
	
	private List<QueryField> createOrGetFields(String fieldName) {
		if(!queryFields.containsKey(fieldName)) {
			queryFields.put(fieldName, new ArrayList<QueryField>());
		}
		
		return queryFields.get(fieldName);
	}
	
	@Override
	public String toString() {
		StringBuilder queryBuilder = new StringBuilder();
		
		queryBuilder.append("(Operation: ").append(type.name()).append(" queryFields: ");
		
		for(List<QueryField> queryFieldList : queryFields.values()) {
			for(QueryField field : queryFieldList) {
				queryBuilder.append(field).append("; ");
			}
		}
		queryBuilder.append(")");
		
		for(QueryBuilder block : blocks) {
			queryBuilder.append("(");
			queryBuilder.append(block);
			queryBuilder.append(")");
		}
		
		return queryBuilder.toString();
	}
}
