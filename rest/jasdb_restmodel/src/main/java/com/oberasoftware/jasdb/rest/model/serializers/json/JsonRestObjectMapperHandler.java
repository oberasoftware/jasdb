package com.oberasoftware.jasdb.rest.model.serializers.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;

import java.io.*;

/**
 * @author Renze de Vries
 */
public class JsonRestObjectMapperHandler implements RestResponseHandler {
    private static final String ENCODING = "UTF8";
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
		try {
			return mapper.readValue(inputStream, dataType);
		} catch (IOException e) {
			throw new RestException("Unable to parse input document: " + e.getMessage());
		}
    }

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, String data) throws RestException {
        try {
            return deserialize(dataType, new ByteArrayInputStream(data.getBytes(ENCODING)));
        } catch (UnsupportedEncodingException e) {
            throw new RestException("Unable to parse input document: " + e.getMessage());
        }
    }

    @Override
    public void serialize(RestEntity entity, OutputStream outputStream) throws RestException {
		try {
			mapper.writeValue(outputStream, entity);
		} catch (IOException e) {
            throw new RestException("Unable to serialize entity", e);
		}
    }

    @Override
    public String getMediaType() {
        return null;
    }
}
