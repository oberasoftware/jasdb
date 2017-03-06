/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine.operations;

import com.oberasoftware.jasdb.engine.BagOperationUtil;
import com.oberasoftware.jasdb.engine.RecordWriterFactoryLoader;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.engine.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * @author Renze de Vries
 */
@Component
@Qualifier("removeOperation")
public class BagRemoveOperation implements DataOperation {

    @Autowired
    private RecordWriterFactoryLoader recordWriterFactoryLoader;

    @Autowired
    private IndexManagerFactory indexManagerFactory;

    @Override
    public void doDataOperation(String instanceId, String bag, SimpleEntity entity) throws JasDBStorageException {
        RecordWriter recordWriter = recordWriterFactoryLoader.loadRecordWriter(instanceId, bag);
        RecordResult recordResult = recordWriter.readRecord(new UUIDKey(entity.getInternalId()));
        if(recordResult.isRecordFound()) {
            SimpleEntity removeEntity = BagOperationUtil.toEntity(recordResult.getStream());
            if(removeEntity != null) {
                recordWriter.removeRecord(new UUIDKey(entity.getInternalId()));
                removeFromIndex(instanceId, bag, removeEntity);
            } else {
                throw new JasDBStorageException("Unable to remove entity with id: " + entity.getInternalId() + ", cannot be deserialed from storage");
            }
        } else {
            throw new JasDBStorageException("Unable to delete entity with id: " + entity.getInternalId() + ", could not be found");
        }
    }

    private void removeFromIndex(String instanceId, String bagName, SimpleEntity removeEntity) throws JasDBStorageException {
        Map<String, Index> indexes = indexManagerFactory.getIndexManager(instanceId).getIndexes(bagName);
        for(Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
            Index index = indexEntry.getValue();

            if(KeyUtil.isAnyDataPresent(removeEntity, index)) {
                Set<Key> removeKeys  = BagOperationUtil.createEntityKeys(removeEntity, index);
                for(Key removeKey : removeKeys) {
                    index.removeFromIndex(removeKey);
                }
            }
        }
    }

    public void setRecordWriterFactoryLoader(RecordWriterFactoryLoader recordWriterFactoryLoader) {
        this.recordWriterFactoryLoader = recordWriterFactoryLoader;
    }

    public void setIndexManagerFactory(IndexManagerFactory indexManagerFactory) {
        this.indexManagerFactory = indexManagerFactory;
    }
}
