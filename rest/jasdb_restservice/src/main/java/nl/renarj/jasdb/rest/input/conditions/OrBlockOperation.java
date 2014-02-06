package nl.renarj.jasdb.rest.input.conditions;

import nl.renarj.jasdb.rest.input.TokenType;

public class OrBlockOperation extends BlockOperation {
	public OrBlockOperation() {
		
	}

	@Override
	public TokenType getTokenType() {
		return TokenType.OR;
	}
}
