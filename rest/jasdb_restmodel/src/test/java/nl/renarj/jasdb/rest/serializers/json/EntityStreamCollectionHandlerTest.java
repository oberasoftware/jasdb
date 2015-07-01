package nl.renarj.jasdb.rest.serializers.json;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.serializers.json.entity.EntityStreamCollectionHandler;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

        SimpleEntity entity = queryResult.next();
        assertNotNull(entity);

        assertEquals("Rotterdam", entity.getProperty("city").getFirstValueObject());

        assertFalse(queryResult.hasNext());
    }
}
