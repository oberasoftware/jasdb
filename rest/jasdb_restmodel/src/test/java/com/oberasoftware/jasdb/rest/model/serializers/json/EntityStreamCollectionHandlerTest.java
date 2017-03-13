package com.oberasoftware.jasdb.rest.model.serializers.json;

import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.rest.model.serializers.json.entity.EntityStreamCollectionHandler;
import com.oberasoftware.jasdb.rest.model.streaming.StreamableEntityCollection;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.*;

/**
 * @author Renze de Vries
 */
public class EntityStreamCollectionHandlerTest {
    @Test
    public void testDeserialize() throws UnsupportedEncodingException, RestException {
        String inputJson = "{\"size\":1,\"timeMilliseconds\":0,\"entities\":[{\"__ID\":\"0000f46d-049c-17e1-0000-013a4c46e5e8\",\"city\":\"Rotterdam\",\"itemId\":399}]}";
        ByteArrayInputStream bis = new ByteArrayInputStream(inputJson.getBytes("UTF8"));

        EntityStreamCollectionHandler handler = new EntityStreamCollectionHandler();
        StreamableEntityCollection collection = handler.deserialize(StreamableEntityCollection.class, bis);

        QueryResult queryResult = collection.getResult();
        assertTrue(queryResult.hasNext());

        Entity entity = queryResult.next();
        assertNotNull(entity);

        assertEquals("Rotterdam", entity.getProperty("city").getFirstValueObject());

        assertFalse(queryResult.hasNext());
    }
}
