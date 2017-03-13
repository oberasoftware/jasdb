package com.oberasoftware.jasdb.rest.model.serializers.json.entity;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.oberasoftware.jasdb.core.serializer.json.JsonEntityDeserializer;
import com.oberasoftware.jasdb.core.serializer.json.JsonEntitySerializer;
import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import com.oberasoftware.jasdb.rest.model.streaming.StreamedEntity;
import com.oberasoftware.jasdb.rest.model.RestEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Renze de Vries
 */
public class EntityHandler implements RestResponseHandler {
    private static final JsonFactory factory = new JsonFactory();

    @Override
    public <T extends RestEntity> T deserialize(Class<T> dataType, InputStream inputStream) throws RestException {
        try {
            try (JsonParser parser = factory.createParser(inputStream)) {
                return dataType.cast(new StreamedEntity(new JsonEntityDeserializer().deserializeEntity(parser)));
            }
        } catch(IOException | MetadataParseException e) {
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

                JsonGenerator generator = factory.createGenerator(outputStream);
                generator.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
                generator.configure(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM, false);

                try {
                    JsonEntitySerializer entitySerializer = new JsonEntitySerializer();
                    entitySerializer.serializeEntity(streamedEntity.getEntity(), generator);
                } finally {
                    generator.close();
                }
            } catch(IOException | MetadataParseException e) {
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
