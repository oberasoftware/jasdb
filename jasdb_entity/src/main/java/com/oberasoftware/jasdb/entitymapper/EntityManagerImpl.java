package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.api.entitymapper.EntityMapper;
import com.oberasoftware.jasdb.api.entitymapper.EntityMetadata;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.EntityBag;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;
import com.oberasoftware.jasdb.api.session.query.QueryExecutor;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class EntityManagerImpl implements EntityManager {
    private static final Logger LOG = getLogger(EntityManagerImpl.class);

    private DBSession session;
    private static EntityMapper ENTITY_MAPPER = new AnnotationEntityMapper();

    public EntityManagerImpl(DBSession session) {
        this.session = session;
    }

    @Override
    public Entity persist(Object persistableObject) throws JasDBStorageException {
        MapResult mappedResult = ENTITY_MAPPER.mapTo(persistableObject);
        String bagName = mappedResult.getBagName();
        EntityBag bag = session.createOrGetBag(bagName);

        Entity persistedEntity;
        try {
            Entity entity = mappedResult.getJasDBEntity();
            if(StringUtils.stringNotEmpty(entity.getInternalId()) && bag.getEntity(entity.getInternalId()) != null) {
                //update
                persistedEntity = bag.updateEntity(mappedResult.getJasDBEntity());
                LOG.debug("Updated entity: {} in bag: {}", persistedEntity, bagName);
            } else {
                persistedEntity = bag.addEntity(mappedResult.getJasDBEntity());
                LOG.debug("Created entity: {} in bag: {}", persistedEntity, bagName);
            }
        } catch(RuntimeJasDBException e) {
            //we do this in case we have exactly two threads at same time trying to persist
            persistedEntity = bag.updateEntity(mappedResult.getJasDBEntity());
            LOG.debug("Updated entity: {} in bag: {}", persistedEntity, bagName);
        }
        //update the ID of the passed object
        ENTITY_MAPPER.updateId(persistedEntity.getInternalId(), persistableObject);

        return persistedEntity;
    }

    @Override
    public void remove(Object persistableObject) throws JasDBStorageException {
        MapResult mappedResult = ENTITY_MAPPER.mapTo(persistableObject);
        String bagName = mappedResult.getBagName();

        EntityBag bag = session.createOrGetBag(bagName);
        bag.removeEntity(mappedResult.getJasDBEntity().getInternalId());
    }

    @Override
    public <T> T findEntity(Class<T> type, String entityId) throws JasDBStorageException {
        EntityMetadata entityMetadata = ENTITY_MAPPER.getEntityMetadata(type);
        EntityBag bag = session.getBag(entityMetadata.getBagName());
        if(bag != null) {
            Entity entity = bag.getEntity(entityId);
            return ENTITY_MAPPER.mapFrom(type, entity);
        }

        return null;
    }

    @Override
    public <T> List<T> findEntities(Class<T> types, QueryBuilder builder) throws JasDBStorageException {
        return findEntities(types, builder, -1, -1);
    }

    @Override
    public <T> List<T> findEntities(Class<T> types, QueryBuilder builder, int limit) throws JasDBStorageException {
        return findEntities(types, builder, -1, limit);
    }

    @Override
    public <T> List<T> findEntities(Class<T> types, QueryBuilder builder, int start, int limit) throws JasDBStorageException {
        List<T> entities = new ArrayList<>();

        EntityMetadata entityMetadata = ENTITY_MAPPER.getEntityMetadata(types);
        EntityBag bag = session.getBag(entityMetadata.getBagName());
        if(bag != null) {
            QueryExecutor executor = bag.find(builder);
            if(start > 0 && limit > 0) {
                executor.paging(start, limit);
            } else if(limit > 0) {
                executor.limit(limit);
            }

            QueryResult result = executor.execute();
            LOG.debug("Executing Query: {} results: {}", builder, result.size());
            for(Entity entity : result) {
                entities.add(ENTITY_MAPPER.mapFrom(types, entity));
            }
        }
        return entities;
    }
}
