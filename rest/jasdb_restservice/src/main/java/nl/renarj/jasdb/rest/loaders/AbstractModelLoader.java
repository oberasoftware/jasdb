/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;

/**
 * User: renarj
 * Date: 3/11/12
 * Time: 10:17 PM
 */
public abstract class AbstractModelLoader implements PathModelLoader {
    @Override
    public boolean isOperationSupported(String operation) {
        return false;
    }

    @Override
    public RestEntity doOperation(InputElement input) throws RestException {
        throw new RestException("Operation not supported on input type");
    }

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        throw new RestException("No remove operation supported");
    }

    @Override
    public RestEntity updateEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        throw new RestException("No update operation supported");
    }
}
