/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service.operations;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.service.BagOperationUtil;

import java.util.Map;
import java.util.Set;

/**
 * User: renarj
 * Date: 5/11/12
 * Time: 3:37 PM
 */
public class BagRemoveOperation implements DataOperation {
    private String bagName;
    private IndexManager indexManager;
    private RecordWriter recordWriter;

    public BagRemoveOperation(String bagName, IndexManager indexManager, RecordWriter recordWriter) {
        this.bagName = bagName;
        this.recordWriter = recordWriter;
        this.indexManager = indexManager;
    }

    @Override
    public void doDataOperation(SimpleEntity entity) throws JasDBStorageException {
        RecordResult recordResult = recordWriter.readRecord(new UUIDKey(entity.getInternalId()));
        if(recordResult.isRecordFound()) {
            SimpleEntity removeEntity = BagOperationUtil.toEntity(recordResult.getStream());
            if(removeEntity != null) {
                recordWriter.removeRecord(new UUIDKey(entity.getInternalId()));
                removeFromIndex(removeEntity);
            } else {
                throw new JasDBStorageException("Unable to remove entity with id: " + entity.getInternalId() + ", cannot be deserialed from storage");
            }
        } else {
            throw new JasDBStorageException("Unable to delete entity with id: " + entity.getInternalId() + ", could not be found");
        }
    }

    private void removeFromIndex(SimpleEntity removeEntity) throws JasDBStorageException {
        Map<String, Index> indexes = indexManager.getIndexes(bagName);
        for(Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
            Index index = indexEntry.getValue();

            if(BagOperationUtil.isDataPresent(removeEntity, index)) {
                Set<Key> removeKeys  = BagOperationUtil.createEntityKeys(removeEntity, index);
                for(Key removeKey : removeKeys) {
                    index.removeFromIndex(removeKey);
                }
            }
        }
    }
}
