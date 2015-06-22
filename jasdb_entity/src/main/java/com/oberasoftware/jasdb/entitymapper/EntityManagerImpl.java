package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.api.entitymapper.EntityMapper;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import org.slf4j.Logger;

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
    public SimpleEntity persist(Object persistableObject) throws JasDBStorageException {
        MapResult mappedResult = ENTITY_MAPPER.mapTo(persistableObject);
        String bagName = mappedResult.getBagName();
        EntityBag bag = session.createOrGetBag(bagName);

        SimpleEntity persistedEntity;
        try {
            SimpleEntity entity = mappedResult.getJasDBEntity();
            if(StringUtils.stringNotEmpty(entity.getInternalId()) && bag.getEntity(entity.getInternalId()) != null) {
                //update
                persistedEntity = bag.updateEntity(mappedResult.getJasDBEntity());
                LOG.debug("Updated entity: {}", persistedEntity);
            } else {
                persistedEntity = bag.addEntity(mappedResult.getJasDBEntity());
                LOG.debug("Created entity: {}", persistedEntity);
            }
        } catch(RuntimeJasDBException e) {
            //we do this in case we have exactly two threads at same time trying to persist
            persistedEntity = bag.updateEntity(mappedResult.getJasDBEntity());
            LOG.debug("Updated entity: {}", persistedEntity);
        }

        return persistedEntity;
    }

    @Override
    public void remove(Object persistableObject) throws JasDBStorageException {
        MapResult mappedResult = ENTITY_MAPPER.mapTo(persistableObject);
        String bagName = mappedResult.getBagName();

        EntityBag bag = session.createOrGetBag(bagName);
        bag.removeEntity(mappedResult.getJasDBEntity().getInternalId());
    }
}
