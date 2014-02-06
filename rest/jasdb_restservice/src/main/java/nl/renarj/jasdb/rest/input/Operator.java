package nl.renarj.jasdb.rest.input;

import nl.renarj.jasdb.api.query.QueryFieldOperator;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;

public enum Operator {
	Equals("=", QueryFieldOperator.EQUALS),
	Larger_than(">", QueryFieldOperator.LARGER_THAN),
	Larger_than_or_Equals(">=", QueryFieldOperator.LARGER_THAN_OR_EQUALS),
	Smaller_than("<", QueryFieldOperator.SMALLER_THAN),
	Smaller_than_or_Equals("<=", QueryFieldOperator.SMALLER_THAN_OR_EQUALS),
    Not_equals("!=", QueryFieldOperator.NOT_EQUALS);
	
	private String token;
	private QueryFieldOperator queryOperator;
	
	Operator(String token, QueryFieldOperator queryOperator) {
		this.token = token;
		this.queryOperator = queryOperator;
	}
	
	public String getToken() {
		return this.token;
	}
	
	public QueryFieldOperator getQueryOperator() {
		return this.queryOperator;
	}
	
	public static Operator getOperator(String requestedToken) throws SyntaxException {
		for(Operator op : values()) {
			if(op.token.equalsIgnoreCase(requestedToken)) {
				return op;
			}
		}
		
		throw new SyntaxException("Unidentified token");
	}
}