package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;

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
