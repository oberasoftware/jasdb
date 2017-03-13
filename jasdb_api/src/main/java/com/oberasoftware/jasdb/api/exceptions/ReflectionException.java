package com.oberasoftware.jasdb.api.exceptions;

public class ReflectionException extends Exception {
	private static final long serialVersionUID = -6228300250989286224L;

	public ReflectionException(String exceptionMessage, Throwable embeddedException) {
		super(exceptionMessage, embeddedException);
	}
	
	public ReflectionException(String exceptionMessage){
		super(exceptionMessage);
	}
}
