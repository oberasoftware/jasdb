/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.serializer.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.session.Value;
import com.oberasoftware.jasdb.core.properties.BooleanValue;
import com.oberasoftware.jasdb.core.properties.EntityValue;
import com.oberasoftware.jasdb.core.properties.IntegerValue;
import com.oberasoftware.jasdb.core.properties.LongValue;
import com.oberasoftware.jasdb.core.serializer.EntitySerializer;

import java.io.IOException;
import java.io.StringWriter;

/**
 * User: Renze de Vries
 */
public class JsonEntitySerializer implements EntitySerializer {
    private static final JsonFactory factory = new JsonFactory();

    @Override
    public String serializeEntity(Entity entity) throws MetadataParseException {
        StringWriter writer = new StringWriter();
        try {
            try (JsonGenerator generator = factory.createGenerator(writer)) {
                serializeEntity(entity, generator);
            }
        } catch(IOException e) {
            throw new MetadataParseException("Unable to serialize entity", e);
        }

        return writer.toString();
    }

    public void serializeEntity(Entity entity, JsonGenerator generator) throws MetadataParseException {
        try {
            generator.writeStartObject();

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
        } catch(IOException e) {
            throw new MetadataParseException("Unable to serialize simple entity: " + e.getClass().getName() + ", " + e.getMessage());
        }
    }


}
