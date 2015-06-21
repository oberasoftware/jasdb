package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.api.entitymapper.EntityMapper;
import com.oberasoftware.jasdb.api.entitymapper.MapResult;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
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
            persistedEntity = bag.addEntity(mappedResult.getJasDBEntity());
            LOG.debug("Created entity: {}", persistedEntity);
        } catch(JasDBStorageException e) {
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
