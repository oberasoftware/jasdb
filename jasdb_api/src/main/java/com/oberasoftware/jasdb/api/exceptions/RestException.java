package com.oberasoftware.jasdb.api.exceptions;

public class RestException extends JasDBException {
	private static final long serialVersionUID = -6620518589377934129L;

	public RestException(String message) {
		super(message);
	}
	
	public RestException(String message, Throwable e) {
		super(message, e);
	}
}
