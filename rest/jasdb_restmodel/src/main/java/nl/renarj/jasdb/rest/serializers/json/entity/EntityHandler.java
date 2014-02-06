package nl.renarj.jasdb.rest.serializers.json.entity;

import nl.renarj.jasdb.api.serializer.json.JsonEntityDeserializer;
import nl.renarj.jasdb.api.serializer.json.JsonEntitySerializer;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamedEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Renze de Vries
 */
public class EntityHandler implements RestResponseHandler {
    private static final Logger LOG = LoggerFactory.getLogger(EntityHandler.class);

    private static final JsonFactory factory = new JsonFactory();

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
        try {
            JsonParser parser = factory.createJsonParser(inputStream);
            try {
                return dataType.cast(new StreamedEntity(new JsonEntityDeserializer().deserializeEntity(parser)));
            } finally {
                parser.close();
            }
        } catch(IOException e) {
            throw new RestException("Unable to parse entity", e);
        } catch(MetadataParseException e) {
            throw new RestException("Unable to parse entity", e);
        }
    }

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, String data) throws RestException {
        JsonEntityDeserializer entityDeserializer = new JsonEntityDeserializer();
        try {
            return dataType.cast(new StreamedEntity(entityDeserializer.deserializeEntity(data)));
        } catch(MetadataParseException e) {
            throw new RestException("Unable to parse entity", e);
        }
    }

    @Override
    public void serialize(RestEntity entity, OutputStream outputStream) throws RestException {
        if(entity instanceof StreamedEntity) {
            try {
                StreamedEntity streamedEntity = (StreamedEntity) entity;

                JsonGenerator generator = factory.createJsonGenerator(outputStream);
                generator.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
                generator.configure(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM, false);

                try {
                    JsonEntitySerializer entitySerializer = new JsonEntitySerializer();
                    entitySerializer.serializeEntity(streamedEntity.getEntity(), generator);
                } finally {
                    generator.close();
                }
            } catch(IOException e) {
                throw new RestException("Unable to serialize entity", e);
            } catch(MetadataParseException e) {
                throw new RestException("Unable to serialize entity", e);
            }
        } else {
            throw new RestException("Unable to serialize entity not of type Entity");
        }

    }

    @Override
    public String getMediaType() {
        return null;
    }
}
