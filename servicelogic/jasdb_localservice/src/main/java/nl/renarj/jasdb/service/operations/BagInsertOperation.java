package nl.renarj.jasdb.service.operations;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.service.BagOperationUtil;

import java.util.Map;
import java.util.Set;

public class BagInsertOperation implements DataOperation {
	private RecordWriter recordWriter;
	private IndexManager indexManager;
	private String bagName;

	public BagInsertOperation(String bagName, IndexManager indexManager, RecordWriter recordWriter) {
		this.recordWriter = recordWriter;
		this.indexManager = indexManager;
		this.bagName = bagName;
	}
	
	@Override
	public void doDataOperation(SimpleEntity entity) throws JasDBStorageException {
        ClonableDataStream entityStream = BagOperationUtil.toStream(entity);
        if(entityStream != null) {
            StatRecord bagWrite = StatisticsMonitor.createRecord("bag:writeRecord");
            recordWriter.writeRecord(new UUIDKey(entity.getInternalId()), entityStream);
            bagWrite.stop();

            StatRecord bagIndexUpdate = StatisticsMonitor.createRecord("bag:indexUpdate");
            insertIntoIndexes(entity);
            bagIndexUpdate.stop();
        } else {
            throw new JasDBStorageException("Invalid entity, can't insert empty entity");
        }
	}

	private void insertIntoIndexes(SimpleEntity entity) throws JasDBStorageException {
		StatRecord getIndexes = StatisticsMonitor.createRecord("bag:getIndexes");
		Map<String, Index> indexes = indexManager.getIndexes(bagName);
		getIndexes.stop();
		
		StatRecord indexIterator = StatisticsMonitor.createRecord("bag:indexForEach");
		for(Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
			Index index = indexEntry.getValue();
			if(BagOperationUtil.isDataPresent(entity, index)) {
                Set<Key> insertKeys = BagOperationUtil.createEntityKeys(entity, index);
				if(!insertKeys.isEmpty()) {
                    BagOperationUtil.doIndexInsert(insertKeys, index);
				}
			}
		}
		indexIterator.stop();
	}

}
