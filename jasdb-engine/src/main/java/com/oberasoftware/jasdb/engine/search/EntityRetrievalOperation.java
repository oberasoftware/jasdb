package com.oberasoftware.jasdb.engine.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.engine.BagOperationUtil;
import com.oberasoftware.jasdb.engine.query.StorageRecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRetrievalOperation {
	private static final Logger LOG = LoggerFactory.getLogger(EntityRetrievalOperation.class);
	
	private RecordWriter<UUIDKey> recordWriter;

	public EntityRetrievalOperation(RecordWriter<UUIDKey> recordWriter) {
		this.recordWriter = recordWriter;
	}
	
	public Entity getEntityById(String entityId) throws JasDBStorageException {
        StatRecord record = StatisticsMonitor.createRecord("findById:RecordRead");
        RecordResult recordResult = recordWriter.readRecord(new UUIDKey(entityId));
        record.stop();

		if(recordResult.isRecordFound()) {
            StatRecord deserializeRecord = StatisticsMonitor.createRecord("findById:deserialize");
			Entity entity = BagOperationUtil.toEntity(recordResult.getStream());
            deserializeRecord.stop();

            return entity;
		} else {
            LOG.debug("Unable to find entity for id: {}", entityId);
            return null;
		}
	}

	public QueryResult getEntities() throws JasDBStorageException {
		return new StorageRecordIterator(recordWriter.getSize(), recordWriter.readAllRecords());
	}

	public QueryResult getEntities(int max) throws JasDBStorageException {
        long size = recordWriter.getSize();
        if(size > max) {
            size = max;
        }

		return new StorageRecordIterator(size, recordWriter.readAllRecords(max));
	}
}
