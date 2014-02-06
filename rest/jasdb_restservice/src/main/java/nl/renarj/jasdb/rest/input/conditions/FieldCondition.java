package nl.renarj.jasdb.rest.input.conditions;

import nl.renarj.jasdb.rest.input.Operator;
import nl.renarj.jasdb.rest.input.TokenType;

public class FieldCondition implements InputCondition {
	public static final String ID_PARAM = "ID";
	private String field;
	private Operator operator;
	private String value;
	
	public FieldCondition(String field, String value) {
		this(field, Operator.Equals, value);
	}
	
	public FieldCondition(String field, Operator operator, String value) {
		this.field = field;
		this.operator = operator;
		this.value = value;
	}
	
	public String getField() {
		return field;
	}

	public Operator getOperator() {
		return operator;
	}

	public String getValue() {
		return value;
	}

	@Override
	public TokenType getTokenType() {
		return TokenType.LITERAL;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Field: ").append(field);
		builder.append(" Operator: ").append(operator);
		builder.append(" Value: ").append(value);
		
		return builder.toString();
	}
}
