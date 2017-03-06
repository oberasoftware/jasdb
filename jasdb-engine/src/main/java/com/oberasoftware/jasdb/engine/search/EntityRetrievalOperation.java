package com.oberasoftware.jasdb.engine.search;

import com.oberasoftware.jasdb.engine.BagOperationUtil;
import com.oberasoftware.jasdb.engine.query.StorageRecordIterator;
import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRetrievalOperation {
	private static final Logger LOG = LoggerFactory.getLogger(EntityRetrievalOperation.class);
	
	private RecordWriter recordWriter;

	public EntityRetrievalOperation(RecordWriter recordWriter) {
		this.recordWriter = recordWriter;
	}
	
	public SimpleEntity getEntityById(String entityId) throws JasDBStorageException {
        StatRecord record = StatisticsMonitor.createRecord("findById:RecordRead");
        RecordResult recordResult = recordWriter.readRecord(new UUIDKey(entityId));
        record.stop();

		if(recordResult.isRecordFound()) {
            StatRecord deserializeRecord = StatisticsMonitor.createRecord("findById:deserialize");
            SimpleEntity entity = BagOperationUtil.toEntity(recordResult.getStream());
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
