package nl.renarj.jasdb.service;

import com.obera.core.concurrency.ResourceLockManager;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.service.operations.BagInsertOperation;
import nl.renarj.jasdb.service.operations.BagRemoveOperation;
import nl.renarj.jasdb.service.operations.BagUpdateOperation;
import nl.renarj.jasdb.service.partitioning.LocalPartitionManager;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import nl.renarj.jasdb.service.search.EntityRetrievalOperation;
import nl.renarj.jasdb.service.search.QuerySearchOperation;
import nl.renarj.jasdb.storage.indexing.IndexScanAndRecovery;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LocalStorageServiceImpl implements StorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageServiceImpl.class);
    public static final String BAG_EXTENSION = ".pjs";
    public static final String FORCE_REBUILD_COMMAND = "forceRebuild";

    private ExecutorService indexRebuilder = Executors.newFixedThreadPool(INDEX_REBUILD_THREADS);

    private static final int INDEX_REBUILD_THREADS = 2;
	private IndexManager indexManager;
    private RecordWriter recordWriter;
    private MetadataStore metadataStore;

    private String instanceId;
    private String bagName;
    private File bagFile;

    protected PartitioningManager partitionManager;
    private IdGenerator generator;
    private ResourceLockManager resourceLockManager = new ResourceLockManager();

	protected LocalStorageServiceImpl(Instance instance, IndexManager indexManager, RecordWriter recordWriter, MetadataStore metadataStore, String bagName) throws JasDBStorageException {
		this.bagFile = new File(instance.getPath(), bagName + BAG_EXTENSION);
        File parentDir = bagFile.getParentFile();
		if(parentDir.exists() || parentDir.mkdirs()) {
            this.recordWriter = recordWriter;
            this.indexManager = indexManager;
            this.metadataStore = metadataStore;
            this.bagName = bagName;
            this.instanceId = instance.getInstanceId();
            this.generator = new MachineGuidGenerator();
            this.partitionManager = new LocalPartitionManager(generator, metadataStore);
        } else {
            throw new JasDBStorageException("Unable to create directory for bag storage file: " + bagFile.toString());
        }
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
            LOGGER.debug("Flushing record storage: {}", recordWriter);
            recordWriter.flush();
            for(Index index : indexManager.getIndexes(bagName).values()) {
                LOGGER.debug("Flushing index: {}", index);
                index.flushIndex();
            }
        } finally {
            resourceLockManager.exclusiveUnlock(false);
        }
    }

    private void closeAndReleaseResources() throws JasDBStorageException {
        indexRebuilder.shutdown();
        recordWriter.closeWriter();
        indexManager.shutdownIndexes();
    }

	@Override
	public void  openService(Configuration configuration) throws JasDBStorageException {
        LOGGER.info("Opening storage service for bag: {}", bagName);
        recordWriter.openWriter();

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
            recordWriter.closeWriter();
            if(!this.bagFile.delete()) {
                this.bagFile.deleteOnExit();
            }
            Collection<Index> indexes = indexManager.getIndexes(bagName).values();
            for(Index index : indexes) {
                indexManager.removeIndex(bagName, index.getName());
            }
        } finally {
            resourceLockManager.exclusiveUnlock(true);
        }
    }

    private void handleIndexScanAndRebuild() throws JasDBStorageException {
        Collection<Index> indexes = indexManager.getIndexes(bagName).values();
        List<Future<?>> indexRebuilds = new ArrayList<Future<?>>(indexes.size());
        LOGGER.info("Doing index scan for: {} items", recordWriter.getSize());
        for(final Index index : indexes) {
            indexRebuilds.add(indexRebuilder.submit(new IndexScanAndRecovery(index, recordWriter.readAllRecords())));
        }
        for(Future<?> indexRebuild : indexRebuilds) {
            try {
                indexRebuild.get();
            } catch(ExecutionException e) {
                throw new JasDBStorageException("Unable to initialize bag, index rebuild failed", e);
            } catch (InterruptedException e) {
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

            new BagInsertOperation(bagName, indexManager, recordWriter).doDataOperation(entity);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}
	
	@Override
	public void removeEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            if(StringUtils.stringNotEmpty(entity.getInternalId())) {
                new BagRemoveOperation(bagName, indexManager, recordWriter).doDataOperation(entity);
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
                new BagUpdateOperation(bagName, indexManager, recordWriter).doDataOperation(entity);
            } else {
                throw new JasDBStorageException("Unable to update record, entity has no id specified");
            }
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public long getSize() throws JasDBStorageException {
		return recordWriter.getSize();
	}

	@Override
	public long getDiskSize() throws JasDBStorageException {
		return recordWriter.getDiskSize();
	}

	@Override
	public SimpleEntity getEntityById(RequestContext requestContext, String id) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(recordWriter).getEntityById(id);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult getEntities(RequestContext context) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(recordWriter).getEntities();
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult getEntities(RequestContext context, int max) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new EntityRetrievalOperation(recordWriter).getEntities(max);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

	@Override
	public QueryResult search(RequestContext context, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
		    return new QuerySearchOperation(bagName, indexManager, recordWriter).search(blockOperation, limit, params);
        } finally {
            resourceLockManager.sharedUnlock();
        }
	}

    @Override
    public void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            Index index = indexManager.createIndex(bagName, indexField, isUnique, valueFields);
            initializeIndex(index);
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    @Override
    public void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            Index index = indexManager.createIndex(bagName, indexField, isUnique, valueFields);
            initializeIndex(index);
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    private void initializeIndex(Index index) throws JasDBStorageException {
        KeyInfo keyInfo = index.getKeyInfo();
        IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.getIndexType());
        if(!metadataStore.containsIndex(instanceId, bagName, definition)) {
            metadataStore.addBagIndex(instanceId, bagName, definition);

            try {
                indexRebuilder.submit(new IndexScanAndRecovery(index, recordWriter.readAllRecords(), true)).get();
            } catch(ExecutionException e) {
                throw new JasDBStorageException("Unable to initialize index, index rebuild failed", e);
            } catch(InterruptedException e) {
                throw new JasDBStorageException("Unable to initialize index, index rebuild failed", e);
            }
        }
    }

    @Override
    public List<String> getIndexNames() throws JasDBStorageException {
        Collection<Index> indexes = indexManager.getIndexes(bagName).values();
        List<String> indexNames = new ArrayList<String>(indexes.size());
        for(Index index : indexes) {
            indexNames.add(index.getName());
        }
        return indexNames;
    }

    @Override
    public void removeIndex(String indexName) throws JasDBStorageException {
        resourceLockManager.sharedLock();
        try {
            Index index = indexManager.getIndex(bagName, indexName);
            if(index != null) {
                KeyInfo keyInfo = index.getKeyInfo();
                IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.getIndexType());

                metadataStore.removeBagIndex(instanceId, bagName, definition);
                indexManager.removeIndex(bagName, indexName);
            } else {
                throw new JasDBStorageException("Unable to remove index with name: " + indexName + ", could not be found");
            }
        } finally {
            resourceLockManager.sharedUnlock();
        }
    }

    @Override
    public String toString() {
        return "LocalStorageServiceImpl{" +
                "bagName='" + bagName + '\'' +
                ", instanceId='" + instanceId + '\'' +
                '}';
    }
}
