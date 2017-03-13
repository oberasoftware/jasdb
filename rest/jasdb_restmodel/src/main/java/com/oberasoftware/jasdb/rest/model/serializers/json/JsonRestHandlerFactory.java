package com.oberasoftware.jasdb.rest.model.serializers.json;

import com.oberasoftware.jasdb.rest.model.streaming.StreamableEntityCollection;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.streaming.StreamedEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import com.oberasoftware.jasdb.rest.model.serializers.json.entity.EntityHandler;
import com.oberasoftware.jasdb.rest.model.serializers.json.entity.EntityStreamCollectionHandler;

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
