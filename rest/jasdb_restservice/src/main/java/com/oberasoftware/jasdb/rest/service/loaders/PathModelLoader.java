package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.service.input.InputElement;
import com.oberasoftware.jasdb.rest.service.input.OrderParam;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;

import java.util.List;

/**
 * The path model loader is responsible for loading and writing to an entity in the model
 */
public interface PathModelLoader {
	String[] getModelNames();
	
	RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext requestContext) throws RestException;
	
	RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    RestEntity updateEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    RestEntity doOperation(InputElement input) throws RestException;
    
    boolean isOperationSupported(String operation);
}
