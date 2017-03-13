package com.oberasoftware.jasdb.rest.service.input.conditions;

import com.oberasoftware.jasdb.rest.service.input.TokenType;

public class AndBlockOperation extends BlockOperation {
	public AndBlockOperation() {
		
	}

	@Override
	public TokenType getTokenType() {
		return TokenType.AND;
	}
}
