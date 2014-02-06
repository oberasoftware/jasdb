package nl.renarj.jasdb.rest.serializers.json;

import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.model.streaming.StreamedEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.rest.serializers.json.entity.EntityHandler;
import nl.renarj.jasdb.rest.serializers.json.entity.EntityStreamCollectionHandler;

/**
 * @author Renze de Vries
 */
public class JsonRestHandlerFactory {
    private static final RestResponseHandler objectMapperHandler = new JsonRestObjectMapperHandler();
    private static final EntityStreamCollectionHandler entityStreamHandler = new EntityStreamCollectionHandler();
    private static final EntityHandler entityHandler = new EntityHandler();

    private JsonRestHandlerFactory() {

    }

    public static RestResponseHandler getRestResponseHandler(Class<? extends RestEntity> restEntity) {
        if(restEntity.equals(StreamableEntityCollection.class)) {
            return entityStreamHandler;
        } else if(restEntity.equals(StreamedEntity.class)) {
            return entityHandler;
        } else {
            return objectMapperHandler;
        }
    }
}
