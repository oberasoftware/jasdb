/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.exceptions;

/**
 * User: Renze de Vries
 * Date: 1/8/12
 * Time: 2:20 PM
 */
public class MetadataParseException extends JasDBStorageException {

    public MetadataParseException(String message, Throwable e) {
        super(message, e);
    }

    public MetadataParseException(String message) {
        super(message);
    }

}
