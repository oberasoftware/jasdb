package nl.renarj.jasdb.rest.serializers.json;

import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * @author Renze de Vries
 */
public class JsonRestObjectMapperHandler implements RestResponseHandler {
    private static final String ENCODING = "UTF8";
    private static final Logger log = LoggerFactory.getLogger(JsonRestObjectMapperHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
		try {
			return mapper.readValue(inputStream, dataType);
		} catch(JsonMappingException e) {
			throw new RestException("Unable to parse input document: " + e.getMessage());
		} catch (JsonParseException e) {
			throw new RestException("Unable to parse input document: " + e.getMessage());
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
		} catch(JsonMappingException e) {
            throw new RestException("Unable to serialize entity", e);
		} catch (JsonGenerationException e) {
            throw new RestException("Unable to serialize entity", e);
		} catch (IOException e) {
            throw new RestException("Unable to serialize entity", e);
		}
    }

    @Override
    public String getMediaType() {
        return null;
    }
}
