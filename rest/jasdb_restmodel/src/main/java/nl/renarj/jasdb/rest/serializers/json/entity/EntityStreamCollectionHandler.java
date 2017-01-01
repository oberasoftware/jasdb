package nl.renarj.jasdb.rest.serializers.json.entity;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.serializer.json.JsonEntitySerializer;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Renze de Vries
 */
public class EntityStreamCollectionHandler implements RestResponseHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityStreamCollectionHandler.class);
    private static final JsonFactory factory = new JsonFactory();
    private static final String COLLECTION_SIZE = "size";
    private static final String COLLECTION_TIME = "timeMilliseconds";
    private static final String COLLECTION_ENTITIES = "entities";

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
        try {
            JsonParser parser = factory.createParser(inputStream);

            JsonToken token = parser.nextToken();
            if(token == JsonToken.START_OBJECT) {
                token = parser.nextToken();
                long size = 0;
                if(token == JsonToken.FIELD_NAME && COLLECTION_SIZE.equals(parser.getText())) {
                    parser.nextToken();
                    size = parser.getNumberValue().longValue();
                }
                token = parser.nextToken();
                long timeMilliseconds = 0;
                if(token == JsonToken.FIELD_NAME && COLLECTION_TIME.equals(parser.getText())) {
                    parser.nextToken();
                    timeMilliseconds = parser.getNumberValue().longValue();
                }
                token = parser.nextToken();
                if(token == JsonToken.FIELD_NAME && COLLECTION_ENTITIES.equals(parser.getText())) {
                    token = parser.nextToken();
                    if(token == JsonToken.START_ARRAY) {
                        log.debug("Parsing a stream of entities of size: {} queried in: {} ms.", size, timeMilliseconds);
                        QueryResult result = new StreamableQueryResult(parser, size, inputStream);
                        return dataType.cast(new StreamableEntityCollection(result));
                    }
                }
            }
            parser.close();
        } catch(IOException e) {
            log.error("Unable to parse streamable entity collection", e);
        }


        throw new RestException("Unable to parse streamable entity collection");
    }

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, String data) throws RestException {
        throw new RestException("Streaming of raw string data not supported");
    }

    @Override
    public void serialize(RestEntity entity, OutputStream outputStream) throws RestException {
        if(entity instanceof StreamableEntityCollection) {
            StreamableEntityCollection entityCollection = (StreamableEntityCollection) entity;

            generateEntityOutput(entityCollection.getSize(), entityCollection.getTimeMilliseconds(), entityCollection.getResult(), outputStream);
        } else {
            throw new RestException("Unable to serialize the entity, not of type: " + StreamableEntityCollection.class.getName());
        }

    }

    private void generateEntityOutput(long size, long timeMilliseconds, QueryResult result, OutputStream outputStream) throws RestException {
        try {
            JsonGenerator generator = factory.createGenerator(outputStream);

            generator.writeStartObject();
            generator.writeNumberField(COLLECTION_SIZE, size);
            generator.writeNumberField(COLLECTION_TIME, timeMilliseconds);
            generator.writeArrayFieldStart(COLLECTION_ENTITIES);

            JsonEntitySerializer entitySerializer = new JsonEntitySerializer();

            for(SimpleEntity entity : result) {
                if(entity != null) {
                    entitySerializer.serializeEntity(entity, generator);
                }
            }

            generator.writeEndArray();

            generator.close();
        } catch(IOException e) {
            throw new RestException("Unable to serialize entity collection", e);
        } catch(MetadataParseException e) {
            throw new RestException("Unable to serialize entity collection: " + e.getMessage());
        }

    }

    @Override
    public String getMediaType() {
        return null;
    }
}
