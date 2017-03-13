package com.oberasoftware.jasdb.engine.operations;

import com.oberasoftware.jasdb.api.engine.IndexManagerFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.engine.BagOperationUtil;
import com.oberasoftware.jasdb.engine.RecordWriterFactoryLoader;
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
	public void doDataOperation(String instanceId, String bag, Entity entity) throws JasDBStorageException {
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

	private void insertIntoIndexes(String instanceId, String bagName, Entity entity) throws JasDBStorageException {
		StatRecord getIndexes = StatisticsMonitor.createRecord("bag:getIndexes");
		Map<String, Index> indexes = indexManagerFactory.getIndexManager(instanceId).getIndexes(bagName);
		getIndexes.stop();
		
		StatRecord indexIterator = StatisticsMonitor.createRecord("bag:indexForEach");
		for(Map.Entry<String, Index> indexEntry : indexes.entrySet()) {
			Index index = indexEntry.getValue();
			if(KeyUtil.isAnyDataPresent(entity, index)) {
                Set<Key> insertKeys = BagOperationUtil.createEntityKeys(entity, index);
				if(!insertKeys.isEmpty()) {
                    BagOperationUtil.doIndexInsert(insertKeys, index);
				}
			}
		}
		indexIterator.stop();
	}

    public void setIndexManagerFactory(IndexManagerFactory indexManagerFactory) {
        this.indexManagerFactory = indexManagerFactory;
    }

    public void setRecordWriterFactory(RecordWriterFactoryLoader recordWriterFactory) {
        this.recordWriterFactory = recordWriterFactory;
    }
}
