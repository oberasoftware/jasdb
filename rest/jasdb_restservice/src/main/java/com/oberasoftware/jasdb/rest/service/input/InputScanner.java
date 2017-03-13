package com.oberasoftware.jasdb.rest.service.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputScanner {
	private static final Logger LOG = LoggerFactory.getLogger(InputScanner.class);

	private static final Pattern tokenPattern = Pattern.compile("'['\\p{L}.\\p{N}- _=:]+'|['\\p{L}.\\p{N}- _]+|[!|=|>|<\\|,()]");
	private Matcher matcher;
	
	public InputScanner(String scanText) {
		this.matcher = tokenPattern.matcher(scanText);
	}
	
	public String nextToken() {
		if(matcher.find()) {
			String matchToken = matcher.group(0);

			LOG.debug("Input token found: {}", matchToken);
			return matchToken;
		} else {
			LOG.trace("End of input, no more tokens found");
			return null;
		}
	}
}
