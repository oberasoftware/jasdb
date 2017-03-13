package com.oberasoftware.jasdb.rest.service.input.conditions;

import com.oberasoftware.jasdb.rest.service.input.TokenType;

public class OrBlockOperation extends BlockOperation {
	public OrBlockOperation() {
		
	}

	@Override
	public TokenType getTokenType() {
		return TokenType.OR;
	}
}
