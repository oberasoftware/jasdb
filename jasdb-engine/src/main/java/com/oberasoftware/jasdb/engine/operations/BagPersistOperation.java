package com.oberasoftware.jasdb.engine.operations;

import com.oberasoftware.jasdb.engine.RecordWriterFactoryLoader;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Component
@Qualifier("persistOperation")
public class BagPersistOperation implements DataOperation {
    private static final Logger LOG = LoggerFactory.getLogger(BagPersistOperation.class);

    @Autowired
    private BagUpdateOperation updateOperation;

    @Autowired
    private BagInsertOperation insertOperation;

    @Autowired
    private RecordWriterFactoryLoader recordWriterFactory;

    @Override
    public void doDataOperation(String instanceId, String bag, SimpleEntity entity) throws JasDBStorageException {
        UUIDKey documentKey = new UUIDKey(entity.getInternalId());
        RecordWriter recordWriter = recordWriterFactory.loadRecordWriter(instanceId, bag);
        RecordResult result = recordWriter.readRecord(documentKey);

        if(result != null && result.isRecordFound()) {
            LOG.debug("Persist operation found existing entity, redirecting to update operation for entity: {}", entity);
            updateOperation.doDataOperation(instanceId, bag, entity);
        } else {
            LOG.debug("No existing entity was found, inserting data: {}", entity);
            insertOperation.doDataOperation(instanceId, bag, entity);
        }
    }

    public void setUpdateOperation(BagUpdateOperation updateOperation) {
        this.updateOperation = updateOperation;
    }

    public void setInsertOperation(BagInsertOperation insertOperation) {
        this.insertOperation = insertOperation;
    }

    public void setRecordWriterFactory(RecordWriterFactoryLoader recordWriterFactory) {
        this.recordWriterFactory = recordWriterFactory;
    }
}
