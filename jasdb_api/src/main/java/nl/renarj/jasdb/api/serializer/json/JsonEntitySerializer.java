/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.serializer.json;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.properties.*;
import nl.renarj.jasdb.api.serializer.EntitySerializer;
import nl.renarj.jasdb.core.exceptions.MetadataParseException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;

/**
 * User: Renze de Vries
 */
public class JsonEntitySerializer implements EntitySerializer {
    private static final JsonFactory factory = new JsonFactory();
    protected static final String PROPERTIES_ARRAY = "properties";
    
    @Override
    public String serializeEntity(SimpleEntity entity) throws MetadataParseException {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = factory.createJsonGenerator(writer);
            try {
                serializeEntity(entity, generator);
            } finally {
                generator.close();
            }
        } catch(IOException e) {
            throw new MetadataParseException("Unable to serialize entity", e);
        }

        return writer.toString();
    }

    public void serializeEntity(SimpleEntity entity, JsonGenerator generator) throws MetadataParseException {
        try {
            generator.writeStartObject();
            if(entity.getInternalId() != null) {
                generator.writeStringField(SimpleEntity.DOCUMENT_ID, entity.getInternalId());
            }
            generator.writeObjectFieldStart(PROPERTIES_ARRAY);
            for(Property property : entity.getProperties()) {
                if(property.hasValues()) {
                    if(property.isMultiValue()) {
                        generator.writeArrayFieldStart(property.getPropertyName());
                    } else {
                        generator.writeFieldName(property.getPropertyName());
                    }

                    for(Value value : property.getValues()) {
                        if(value instanceof LongValue) {
                            generator.writeNumber(((LongValue) value).toLong());
                        } else if(value instanceof IntegerValue) {
                            generator.writeNumber(((IntegerValue)value).toInteger());
                        } else if(value instanceof BooleanValue) {
                            generator.writeBoolean(((BooleanValue)value).toBoolean());
                        } else if(value instanceof EntityValue) {
                            serializeEntity(((EntityValue) value).toEntity(), generator);
                        } else {
                            generator.writeString(value.toString());
                        }
                    }

                    if(property.isMultiValue()) {
                        generator.writeEndArray();
                    }
                }
            }
            generator.writeEndObject();

            generator.writeEndObject();
        } catch(IOException e) {
            throw new MetadataParseException("Unable to serialize simple entity: " + e.getClass().getName() + ", " + e.getMessage());
        }
    }


}
