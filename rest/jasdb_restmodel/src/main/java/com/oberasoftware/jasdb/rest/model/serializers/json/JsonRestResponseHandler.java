package com.oberasoftware.jasdb.rest.model.serializers.json;

import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import com.oberasoftware.jasdb.rest.model.RestEntity;

import java.io.InputStream;
import java.io.OutputStream;

public class JsonRestResponseHandler implements RestResponseHandler {
	@Override
	public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
        return JsonRestHandlerFactory.getRestResponseHandler(dataType).deserialize(dataType, inputStream);
	}

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, String data) throws RestException {
        return JsonRestHandlerFactory.getRestResponseHandler(dataType).deserialize(dataType, data);
    }

	@Override
	public void serialize(RestEntity data, OutputStream outputStream) throws RestException {
        JsonRestHandlerFactory.getRestResponseHandler(data.getClass()).serialize(data, outputStream);
	}

	@Override
	public String getMediaType() {
		return "application/json;charset=utf-8";
	}

}
