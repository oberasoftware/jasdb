package nl.renarj.jasdb.service.operations;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.service.BagOperationUtil;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

@Component
@Qualifier("insertOperation")
public class BagInsertOperation implements DataOperation {

    @Autowired
    private IndexManagerFactory indexManagerFactory;

    @Autowired
    private RecordWriterFactoryLoader recordWriterFactory;

	@Override
	public void doDataOperation(String instanceId, String bag, SimpleEntity entity) throws JasDBStorageException {
        ClonableDataStream entityStream = BagOperationUtil.toStream(entity);
        if(entityStream != null) {
            StatRecord bagWrite = StatisticsMonitor.createRecord("bag:writeRecord");
            recordWriterFactory.loadRecordWriter(instanceId, bag).writeRecord(new UUIDKey(entity.getInternalId()), entityStream);
            bagWrite.stop();

            StatRecord bagIndexUpdate = StatisticsMonitor.createRecord("bag:indexUpdate");
            insertIntoIndexes(instanceId, bag, entity);
            bagIndexUpdate.stop();
        } else {
            throw new JasDBStorageException("Invalid entity, can't insert empty entity");
        }
	}

	private void insertIntoIndexes(String instanceId, String bagName, SimpleEntity entity) throws JasDBStorageException {
		StatRecord getIndexes = StatisticsMonitor.createRecord("bag:getIndexes");
		Map<String, Index> indexes = indexManagerFactory.getIndexManager(instanceId).getIndexes(bagName);
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
