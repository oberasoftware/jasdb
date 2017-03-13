/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.exceptions;

public class DatastoreException extends JasDBStorageException {
	private static final long serialVersionUID = 3773070907437415917L;

	public DatastoreException(String message, Throwable e) {
		super(message, e);
	}
	
	public DatastoreException(String message) {
		super(message);
	}
}
