package nl.renarj.jasdb.api.serializer.json;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.serializer.EntityDeserializer;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Renze de Vries
 */
public class JsonEntityDeserializer implements EntityDeserializer {
    private static final JsonFactory factory = new JsonFactory();

    @Override
    public SimpleEntity deserializeEntity(String serializedEntity) throws MetadataParseException {
        if(StringUtils.stringNotEmpty(serializedEntity)) {
            try {
                JsonParser parser = factory.createParser(serializedEntity);

                return deserializeEntity(parser);
            } catch(IOException e) {
                throw new MetadataParseException("Unable to parse entity", e);
            }
        }

        throw new MetadataParseException("Unable to parse entity metadata, empty content");
    }

    @Override
    public SimpleEntity deserializeEntity(InputStream stream) throws MetadataParseException {
        try {
            JsonParser parser = factory.createParser(stream);

            return deserializeEntity(parser);
        } catch(IOException e) {
            throw new MetadataParseException("Unable to parse entity", e);
        }
    }

    public SimpleEntity deserializeEntity(JsonParser parser) throws MetadataParseException {
        try {
            SimpleEntity entity = new SimpleEntity();
            entity = handleProperties(parser, entity);
            parser.nextToken();
            return entity;
        } catch(IOException e) {
            throw new MetadataParseException("Unable to parse entity", e);
        }
    }

    private SimpleEntity handleProperties(JsonParser parser, SimpleEntity entity) throws IOException, MetadataParseException {
        JsonToken token = parser.getCurrentToken() == JsonToken.START_OBJECT ? parser.getCurrentToken() : parser.nextToken();
        if(token == JsonToken.START_OBJECT) {
            token = parser.nextToken();
            while(token != JsonToken.END_OBJECT) {
                assertToken(JsonToken.FIELD_NAME, token);
                String fieldName = parser.getCurrentName();

                if(SimpleEntity.DOCUMENT_ID.equals(fieldName)) {
                    token = parser.nextToken();
                    if(token != JsonToken.VALUE_NULL) {
                        String id = parser.getText();
                        entity.setInternalId(id);
                    }
                    parser.nextToken();
                } else {
                    token = parser.nextToken(); // start values
                    if(token == JsonToken.START_ARRAY) { //multivalues
                        parser.nextToken(); //we need to advance by one for first value
                        handleValues(parser, fieldName, entity);
                        parser.nextToken();
                    } else {
                        handleValues(parser, fieldName, entity);
                    }
                }
                token = parser.getCurrentToken();
            }
            return entity;
        } else {
            return null;
        }
    }

    private void handleValues(JsonParser parser, String field, SimpleEntity entity) throws IOException, MetadataParseException {
        JsonToken token = parser.getCurrentToken();
        while(token != JsonToken.END_ARRAY && token != JsonToken.FIELD_NAME && token != JsonToken.END_OBJECT) {
            if(token.isNumeric()) {
                long value = parser.getLongValue();
                entity.addProperty(field, value);
            } else if(token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
                entity.addProperty(field, parser.getBooleanValue());
            } else if(token == JsonToken.START_OBJECT) {
                EmbeddedEntity embeddedEntity = new EmbeddedEntity();
                if(handleProperties(parser, embeddedEntity) != null) {
                    entity.addEntity(field, embeddedEntity);
                }
            } else if(token == JsonToken.VALUE_STRING) {
                String value = parser.getText();
                if(SimpleEntity.DOCUMENT_ID.equals(field)) {
                    entity.setInternalId(value);
                } else {
                    entity.addProperty(field, value);
                }
            } else if(token != JsonToken.VALUE_NULL) {
                throw new MetadataParseException("Unexpected token in entity: " + token);
            }
            token = parser.nextToken();
        }

    }

    private void assertToken(JsonToken expected, JsonToken actual) throws MetadataParseException {
        if(expected != actual) {
            throw new MetadataParseException("Unable to parse entity metadata, expected token: " + expected + " but got token: " + actual);
        }
    }

}
