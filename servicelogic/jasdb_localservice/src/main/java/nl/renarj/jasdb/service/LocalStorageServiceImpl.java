package nl.renarj.jasdb.service;

import com.oberasoftware.core.concurrency.ResourceLockManager;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.service.operations.DataOperation;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import nl.renarj.jasdb.service.search.EntityRetrievalOperation;
import nl.renarj.jasdb.service.search.QuerySearchOperation;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;
import nl.renarj.jasdb.storage.indexing.IndexScanAndRecovery;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;
import nl.renarj.jasdb.storage.transactional.TransactionalRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component("LocalStorageService")
@Scope("prototype")
public class LocalStorageServiceImpl implements StorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageServiceImpl.class);
    public static final String BAG_EXTENSION = ".pjs";
    public static final String FORCE_REBUILD_COMMAND = "forceRebuild";

    private ExecutorService indexRebuilder = Executors.newFixedThreadPool(INDEX_REBUILD_THREADS);

    private static final int INDEX_REBUILD_THREADS = 2;

    @Autowired
    private RecordWriterFactoryLoader recordWriterFactoryLoader;

    private String instanceId;
    private String bagName;

    @Autowired
    private PartitioningManager partitionManager;

    @Autowired
    private IdGenerator generator;

    private ResourceLockManager resourceLockManager = new ResourceLockManager();

    @Autowired
    @Qualifier("insertOperation")
    private DataOperation bagInsertOperation;

    @Autowired
    @Qualifier("removeOperation")
    private DataOperation bagRemoveOperation;

    @Autowired
    @Qualifier("updateOperation")
    private DataOperation bagUpdateOperation;

    @Autowired
    private IndexManagerFactory indexManagerFactory;

    @Autowired
    private MetadataStore metadataStore;

    public LocalStorageServiceImpl(String instanceId, String bagName) {
        this.bagName = bagName;
        this.instanceId = instanceId;
    }

    @Override
    public String getBagName() {
        return this.bagName;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
	public void closeService() throws JasDBStorageException {
        resourceLockManager.exclusiveLock();
        try {
            closeAndReleaseResources();
        } finally {
            resourceLockManager.exclusiveUnlock(true);
        }
	}

    @Override
    public void flush() throws JasDBStorageException {
        resourceLockManager.exclusiveLock();
        try {
            LOGGER.debug("Flushing bag data: {}", bagName);
            recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName).flush();
            indexManagerFactory.getIndexManager(instanceId).flush(bagName);
        } finally {
            resourceLockManager.exclusiveUnlock(false);
        }
    }

    private void closeAndReleaseResources() throws JasDBStorageException {
        indexRebuilder.shutdown();
    }

	@Override
	public void  openService(Configuration configuration) throws JasDBStorageException {
        LOGGER.info("Opening storage service for bag: {}", bagName);

        if(!recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName).isOpen()) {
            throw new ConfigurationException("Unable to open record writer for instance/bag: " + instanceId + '/' + bagName);
        }

        if(!metadataStore.isLastShutdownClean() || Boolean.parseBoolean(System.getProperty(FORCE_REBUILD_COMMAND))) {
            LOGGER.info("Previous shutdown of: {} was unclean or forced rebuild triggered, scanning and rebuilding indexes", this);
            handleIndexScanAndRebuild();
        }
        LOGGER.info("Finished opening storage service for bag: {}", bagName);
	}

    @Override
    public void remove() throws JasDBStorageException {
        resourceLockManager.exclusiveLock();
        try {
            indexRebuilder.shutdown();
            recordWriterFactoryLoader.remove(instanceId, bagName);

            IndexManager indexManager = indexManagerFactory.getIndexManager(instanceId);
            Collection<Index> indexes = indexManager.getIndexes(bagName).values();
            for(Index index : indexes) {
                indexManager.removeIndex(bagName, index.getName());
            }
        } finally {
            resourceLockManager.exclusiveUnlock(true);
        }
    }

    private void handleIndexScanAndRebuild() throws JasDBStorageException {
        Collection<Index> indexes = getIndexManager().getIndexes(bagName).values();
        List<Future<?>> indexRebuilds = new ArrayList<>(indexes.size());
        LOGGER.info("Doing index scan for: {} items", getSize());
        RecordWriter recordWriter = recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName);
        if(recordWriter instanceof TransactionalRecordWriter) {
            TransactionalRecordWriter transactionalRecordWriter = (TransactionalRecordWriter) recordWriter;
            LOGGER.info("Forcing primary key rebuild first, we need to ensure integrity");
            transactionalRecordWriter.verify(recordResult -> {
                try {
                    return new UUIDKey(BagOperationUtil.toEntity(recordResult.getStream()).getInternalId());
                } catch (JasDBStorageException e) {
                    throw new RuntimeJasDBException("Unable to read jasdb entitiy", e);
                }
            });
        }

        LOGGER.info("Doing index scans");
        for(final Index index : indexes) {
            indexRebuilds.add(indexRebuilder.submit(new IndexScanAndRecovery(index, getRecordWriter().readAllRecords())));
        }
        for(Future<?> indexRebuild : indexRebuilds) {
            try {
                indexRebuild.get();
            } catch(ExecutionException | InterruptedException e) {
                throw new JasDBStorageException("Unable to initialize bag, index rebuild failed", e);
            }
        }
    }

    @Override
    public PartitioningManager getPartitionManager() {
        return partitionManager;
    }

    @Override
    public void initializePartitions() throws JasDBStorageException {
        partitionManager.initializePartitions();
    }

    @Override
	public void insertEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            if(entity.getInternalId() == null || entity.getInternalId().isEmpty()) {
                entity.setInternalId(generator.generateNewId());
            }

            bagInsertOperation.doDataOperation(instanceId, bagName, entity);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}
	
	@Override
	public void removeEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            if(StringUtils.stringNotEmpty(entity.getInternalId())) {
                bagRemoveOperation.doDataOperation(instanceId, bagName, entity);
            } else {
                throw new JasDBStorageException("Unable to remove record, entity has no id specified");
            }
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

    @Override
    public void removeEntity(RequestContext context, String internalId) throws JasDBStorageException {
        removeEntity(context, new SimpleEntity(internalId));
    }

    @Override
	public void updateEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            if(StringUtils.stringNotEmpty(entity.getInternalId())) {
                bagUpdateOperation.doDataOperation(instanceId, bagName, entity);
            } else {
                throw new JasDBStorageException("Unable to update record, entity has no id specified");
            }
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public long getSize() throws JasDBStorageException {
		return recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName).getSize();
	}

	@Override
	public long getDiskSize() throws JasDBStorageException {
		return recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName).getDiskSize();
	}

	@Override
	public SimpleEntity getEntityById(RequestContext requestContext, String id) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(getRecordWriter()).getEntityById(id);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult getEntities(RequestContext context) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(getRecordWriter()).getEntities();
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult getEntities(RequestContext context, int max) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(getRecordWriter()).getEntities(max);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult search(RequestContext context, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new QuerySearchOperation(bagName, getIndexManager(), getRecordWriter()).search(blockOperation, limit, params);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

    @Override
    public void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            Index index = getIndexManager().createIndex(bagName, indexField, isUnique, valueFields);
            initializeIndex(index);
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    @Override
    public void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            Index index = getIndexManager().createIndex(bagName, indexField, isUnique, valueFields);
            initializeIndex(index);
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    private void initializeIndex(Index index) throws JasDBStorageException {
        KeyInfo keyInfo = index.getKeyInfo();
        IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.getIndexType());
        if(metadataStore.containsIndex(instanceId, bagName, definition)) {
            try {
                indexRebuilder.submit(new IndexScanAndRecovery(index, getRecordWriter().readAllRecords(), true)).get();
            } catch(ExecutionException | InterruptedException e) {
                throw new JasDBStorageException("Unable to initialize index, index rebuild failed", e);
            }
        } else {
            throw new JasDBStorageException("Cannot initialize index, does not exist in store");
        }
    }

    @Override
    public List<String> getIndexNames() throws JasDBStorageException {
        Collection<Index> indexes = getIndexManager().getIndexes(bagName).values();
        List<String> indexNames = new ArrayList<>(indexes.size());
        for(Index index : indexes) {
            indexNames.add(index.getName());
        }
        return indexNames;
    }

    @Override
    public void removeIndex(String indexName) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            IndexManager indexManager = getIndexManager();
            indexManager.removeIndex(bagName, indexName);
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    private IndexManager getIndexManager() throws JasDBStorageException {
        return indexManagerFactory.getIndexManager(instanceId);
    }

    private RecordWriter getRecordWriter() throws JasDBStorageException {
        return recordWriterFactoryLoader.loadRecordWriter(instanceId, bagName);
    }

    @Override
    public String toString() {
        return "LocalStorageServiceImpl{" +
                "bagName='" + bagName + '\'' +
                ", instanceId='" + instanceId + '\'' +
                '}';
    }

    public void setRecordWriterFactoryLoader(RecordWriterFactoryLoader recordWriterFactoryLoader) {
        this.recordWriterFactoryLoader = recordWriterFactoryLoader;
    }

    public void setPartitionManager(PartitioningManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public void setGenerator(IdGenerator generator) {
        this.generator = generator;
    }

    public void setBagInsertOperation(DataOperation bagInsertOperation) {
        this.bagInsertOperation = bagInsertOperation;
    }

    public void setBagRemoveOperation(DataOperation bagRemoveOperation) {
        this.bagRemoveOperation = bagRemoveOperation;
    }

    public void setBagUpdateOperation(DataOperation bagUpdateOperation) {
        this.bagUpdateOperation = bagUpdateOperation;
    }

    public void setIndexManagerFactory(IndexManagerFactory indexManagerFactory) {
        this.indexManagerFactory = indexManagerFactory;
    }

    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
}
