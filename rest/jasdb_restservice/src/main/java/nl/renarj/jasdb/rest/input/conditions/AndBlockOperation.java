package nl.renarj.jasdb.rest.input.conditions;

import nl.renarj.jasdb.rest.input.TokenType;

public class AndBlockOperation extends BlockOperation {
	public AndBlockOperation() {
		
	}

	@Override
	public TokenType getTokenType() {
		return TokenType.AND;
	}
}
