package com.oberasoftware.jasdb.rest.service.input;

import com.oberasoftware.jasdb.api.exceptions.SyntaxException;
import com.oberasoftware.jasdb.rest.service.input.conditions.BlockOperation;
import com.oberasoftware.jasdb.rest.service.input.conditions.FieldCondition;
import com.oberasoftware.jasdb.rest.service.input.conditions.InputCondition;
import com.oberasoftware.jasdb.rest.service.input.conditions.OrBlockOperation;
import com.oberasoftware.jasdb.rest.service.input.conditions.AndBlockOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputParser {
	private static final Logger LOG = LoggerFactory.getLogger(InputParser.class);
    private static final String STRING_MARKER = "'";
	
	private InputScanner scanner;
	
	public InputParser(InputScanner scanner) {
		this.scanner = scanner;
	}
	
	public InputCondition getCondition() throws SyntaxException {
		return parse(false);
	}
	
	private InputCondition parse(boolean expectBlockEnd) throws SyntaxException {
		InputCondition parentCondition = null;
		String token;
		while((token = scanner.nextToken()) != null) {
			TokenType tokenType = TokenType.getTokenType(token);
			
			switch(tokenType) {
				case OR:
				case AND:					
					parentCondition = handleBlockOperation(tokenType, parentCondition);
				break;
				case BLOCK_START:
					LOG.trace("Block start detected");
                    if(parentCondition != null) {
					    parentCondition = blockStart(parentCondition);
                    } else {
                        parentCondition = parse(true);
                    }
				break;
				case BLOCK_END:
					if(expectBlockEnd) {
						LOG.trace("Block end detected");
						return parentCondition;
					} else {
						throw new SyntaxException("Unexpected block end detected");
					}
				case LITERAL:
					parentCondition = handleLiteral(token, parentCondition);
					LOG.trace("Detected a field condition: {}", parentCondition);
			}
		}
		if(!expectBlockEnd || TokenType.getTokenType(token) == TokenType.BLOCK_END) {
			return parentCondition;
		} else {
			throw new SyntaxException("Unexpected end of input, expected block close");
		}
		
	}
	
	private InputCondition handleBlockOperation(TokenType type, InputCondition previous) throws SyntaxException {
		if(previous != null) {
			TokenType previousTokenType = previous.getTokenType();
			if(previousTokenType == TokenType.LITERAL) {
				FieldCondition condition = (FieldCondition) previous;
				
				BlockOperation blockOperation = createBlockOperation(type);
				blockOperation.addInputCondition(condition);
				return blockOperation;
			} else if (isBlockOperation(previousTokenType)) {
				BlockOperation previousBlockOperation = (BlockOperation) previous;
				
				if(previousBlockOperation.getTokenType() == type) {
					return previousBlockOperation;
				} else {
					BlockOperation blockOperation = createBlockOperation(type);
					blockOperation.addInputCondition(previousBlockOperation);
					
					return blockOperation;
				}				
			} else {
				throw new SyntaxException("Invalid Query syntax, invalid token before operator: " + type);
			}
		} else {
			throw new SyntaxException("Invalid Query syntax, cannot start with operator: " + type);
		}
	}
	
	private BlockOperation createBlockOperation(TokenType type) {
		if(type == TokenType.OR) {
			return new OrBlockOperation();
		} else {
			return new AndBlockOperation();
		}
	}
	
	private InputCondition blockStart(InputCondition previous) throws SyntaxException {
		TokenType previousTokenType = previous.getTokenType();
		if(isBlockOperation(previousTokenType)) {
			BlockOperation blockOperation = (BlockOperation) previous;
			
			InputCondition blockResult = parse(true);
			blockOperation.addInputCondition(blockResult);
			
			return blockOperation;
		} else {
			throw new SyntaxException("Invalid Query syntax, block started without operator");
		}
		
	}
	
	private InputCondition handleLiteral(String token, InputCondition parent) throws SyntaxException {
		if(parent != null) {
			TokenType previousTokenType = parent.getTokenType();
			if(isBlockOperation(previousTokenType)) {
				BlockOperation blockOperation = (BlockOperation) parent;
				blockOperation.addInputCondition(decodeFieldCondition(token));
				
				return blockOperation;
			} else {
				throw new SyntaxException("Invalid Query syntax, token: " + token + " unexpected");
			}
		} else {
			return decodeFieldCondition(token);
		}
	}
	
	private InputCondition decodeFieldCondition(String token) throws SyntaxException {
		TokenType currentTokenType = TokenType.getTokenType(token);
		if(currentTokenType == TokenType.LITERAL) {
			String operatorToken = scanner.nextToken();
			if(operatorToken != null) {
				return decodeFieldOperatorCondition(token, operatorToken);
			} else {
				return new FieldCondition(FieldCondition.ID_PARAM, token);
			}
		} else {
			throw new SyntaxException("Unexpected token type: " + token);
		}
	}
	
	private InputCondition decodeFieldOperatorCondition(String fieldToken, String operatorToken) throws SyntaxException {
		TokenType operatorTokenType = TokenType.getTokenType(operatorToken);
		if(operatorTokenType.isFieldOperator()) {
			String valueToken = scanner.nextToken();
			TokenType valueTokenType = TokenType.getTokenType(valueToken);
			if(valueTokenType == TokenType.EQUALS) {
				valueToken = scanner.nextToken();
				valueTokenType = TokenType.getTokenType(valueToken);
				if(valueToken != null && valueTokenType == TokenType.LITERAL) {
					operatorToken = operatorToken + TokenType.EQUALS.getToken();
					
					return new FieldCondition(fieldToken, Operator.getOperator(operatorToken), decodeValue(valueToken));
				} else {
					throw new SyntaxException("Unexpected token: " + valueToken);	
				}
			} else if(valueToken != null && valueTokenType == TokenType.LITERAL) {
				return new FieldCondition(fieldToken, Operator.getOperator(operatorToken), decodeValue(valueToken));
			} else if(valueToken == null) {
                throw new SyntaxException("No field value specified");
            } else {
				throw new SyntaxException("Unexpected token: " + valueTokenType);
			}					
		} else {
			throw new SyntaxException("Unexpected token: " + operatorToken);
		}
	}

    private String decodeValue(String valueToken) throws SyntaxException {
        if(valueToken.startsWith(STRING_MARKER) && valueToken.endsWith(STRING_MARKER)) {
            return valueToken.substring(1, valueToken.length() - 1);
        } else {
            return valueToken;
        }
    }

	private boolean isBlockOperation(TokenType type) {
		return (type == TokenType.AND || type == TokenType.OR);
	}
}
