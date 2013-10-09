package nl.renarj.jasdb.rest.input;

import nl.renarj.jasdb.rest.input.conditions.InputCondition;
import nl.renarj.jasdb.rest.model.RestEntity;

public class InputElement {
	private String elementName;
	private InputCondition condition;
	private InputElement previous;
	private InputElement next;
	private RestEntity result;
	
	public InputElement(String elementName) {
		this.elementName = elementName;
	}
	
	public RestEntity getResult() {
		return result;
	}

	public void setResult(RestEntity result) {
		this.result = result;
	}

	public String getElementName() {
		return elementName;
	}
	
	public InputElement getPrevious() {
		return previous;
	}

	public void setPrevious(InputElement previous) {
		this.previous = previous;
	}

	public InputElement getNext() {
		return next;
	}

	public void setNext(InputElement next) {
		this.next = next;
	}

	public InputCondition getCondition() {
		return condition;
	}

	public InputElement setCondition(InputCondition condition) {
		this.condition = condition;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		return builder.toString();
	}
}
