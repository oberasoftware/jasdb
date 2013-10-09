package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;

import java.util.List;

public interface PathModelLoader {
	public String[] getModelNames();
	
	public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext requestContext) throws RestException;
	
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    public RestEntity updateEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException;

    public RestEntity doOperation(InputElement input) throws RestException;
    
    public boolean isOperationSupported(String operation);
}
