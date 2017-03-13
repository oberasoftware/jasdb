package com.oberasoftware.jasdb.rest.service.input.conditions;

import java.util.ArrayList;
import java.util.List;

public abstract class BlockOperation implements InputCondition {
	private List<InputCondition> childConditions;
	
	public BlockOperation() {
		this.childConditions = new ArrayList<>();
	}
	
	public BlockOperation addInputCondition(InputCondition condition) {
		this.childConditions.add(condition);
		return this;
	}
	
	public List<InputCondition> getChildConditions() {
		return this.childConditions;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getTokenType()).append("(");
		boolean first = true;
		for(InputCondition condition : childConditions) {
			if(!first) {
				builder.append(", ");
			}
			builder.append("Condition[").append(condition).append("]");
			first = false;
		}
		builder.append(")");
		
		return builder.toString();
	}
}
