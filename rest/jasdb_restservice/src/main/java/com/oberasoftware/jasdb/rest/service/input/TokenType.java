package com.oberasoftware.jasdb.rest.service.input;

public enum TokenType {
	BLOCK_START("(", false),
	BLOCK_END(")", false),
	AND(",", false),
	OR("|", false),
	EQUALS("=", true),
	LARGER(">", true),
	SMALLER("<", true),
    NOT_EQUALS("!", true),
	LITERAL("", false);
	
	
	private String token;
    private boolean fieldOperator;
	
	TokenType(String token, boolean fieldOperator) {
		this.token = token;
        this.fieldOperator = fieldOperator;
	}
	
	public String getToken() {
		return this.token;
	}
	
	public static TokenType getTokenType(String token) {
		for(TokenType type : values()) {
			if(type.getToken().equals(token)) {
				return type;
			}
		}
		
		return TokenType.LITERAL;
	}

    public boolean isFieldOperator() {
        return fieldOperator;
    }
}
