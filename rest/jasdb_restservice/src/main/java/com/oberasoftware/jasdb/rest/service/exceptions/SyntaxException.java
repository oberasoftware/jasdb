package com.oberasoftware.jasdb.rest.service.exceptions;

import com.oberasoftware.jasdb.api.exceptions.RestException;

public class SyntaxException extends RestException {
	private static final long serialVersionUID = 1935231255078020632L;

	public SyntaxException(String message) {
		super(message);
	}
}
