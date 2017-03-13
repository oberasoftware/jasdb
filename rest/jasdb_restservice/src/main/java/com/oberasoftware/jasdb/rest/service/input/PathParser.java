package com.oberasoftware.jasdb.rest.service.input;

import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.rest.service.input.conditions.InputCondition;
import com.oberasoftware.jasdb.api.exceptions.SyntaxException;
import com.oberasoftware.jasdb.rest.service.input.conditions.AndBlockOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PathParser implements Iterable<InputElement>, Iterator<InputElement> {
	private static final Pattern elementPattern = Pattern.compile("(\\w+)\\((.*)\\)|(\\w+)");
	
	private static final Logger LOG = LoggerFactory.getLogger(PathParser.class);
	
	private static final String PATH_SEPERATOR = "/";
	
	private String path;
	private List<InputElement> pathElements;
	private int currentIndex = 0;
	
	public PathParser(String path) throws SyntaxException {
	    if(path.startsWith("/")) {
	        this.path = path.substring(1);
        } else {
            this.path = path;
        }
		pathElements = new LinkedList<>();
		parse();
	}
	
	private void parse() throws SyntaxException {
		LOG.debug("Parsing request path: [{}]", this.path);
		InputElement previousElement = null;
		
		for(String pathElement : path.split(PATH_SEPERATOR)) {
			LOG.debug("Pathelement: {}", pathElement);
			Matcher matcher = elementPattern.matcher(pathElement);
			if(matcher.find()) {
				String elementName = matcher.group(1);
				String elementConditions = matcher.group(2);
				String simpleElementName = matcher.group(3);
				
				InputElement inputElement = null;
				if(StringUtils.stringNotEmpty(elementName)) {
					LOG.trace("Element: {} with conditions: {}", elementName, elementConditions);
					inputElement = new InputElement(elementName);
					int expectedLength = elementName.length() + elementConditions.length() + 2;
					if(expectedLength == pathElement.length()) {
						InputCondition condition = new InputParser(new InputScanner(elementConditions)).getCondition();
						if(condition != null) {
							inputElement.setCondition(condition);
						} else {
                            inputElement.setCondition(new AndBlockOperation());
						}
					} else {
						throw new SyntaxException("Invalid path syntax: " + pathElement);
					}
				} else if(StringUtils.stringNotEmpty(simpleElementName)) {
					LOG.trace("Element: {} without conditions", simpleElementName);
					if(pathElement.contains("(") || pathElement.contains(")")) {
						throw new SyntaxException("The path element contains invalid block open or close brackets: " + pathElement);
					} else {
						inputElement = new InputElement(simpleElementName);
					}
				} else {
					throw new SyntaxException("Invalid element syntax: " + pathElement);
				}
				
				inputElement.setPrevious(previousElement);
				if(previousElement != null) {
					previousElement.setNext(inputElement);
				}
				previousElement = inputElement;
				pathElements.add(inputElement);
			} else {
				throw new SyntaxException("Invalid path syntax: " + pathElement);
			}
		}
	}
	
	@Override
	public Iterator<InputElement> iterator() {
		return pathElements.iterator();
	}

	public boolean hasNext() {
		return currentIndex < pathElements.size(); 
	}

	public InputElement next() {
		if(hasNext()) {
			return pathElements.get(currentIndex++);
		} else {
			return null;
		}
	}

	public void remove() {
		throw new RuntimeException("The remove on path parser is not implemented");
	}	
}
