package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.storage.RecordStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BagInfoReaderWriter implements BagInfoReader {
    private Logger log = LoggerFactory.getLogger(BagInfoReaderWriter.class);
    
    private enum MetadataType {
        INDEX_DATA("_index"),
        PARTITION_DATA("_partition");
        
        private String type;
        
        MetadataType(String type) {
            this.type = type;
        }
        
        public String getType() {
            return this.type;
        }
    }
    
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
	private RecordWriter recordWriter;
    private String bagName;
    
    private Map<String, IndexDefinitionWrapper> indexDefinitions;
    private Map<String, BagPartitionRecordWrapper> bagPartitions;

	public BagInfoReaderWriter(String bag, RecordWriter recordWriter) throws JasDBStorageException {
        this.indexDefinitions = new HashMap<String, IndexDefinitionWrapper>();
        this.bagPartitions = new HashMap<String, BagPartitionRecordWrapper>();
		this.recordWriter = recordWriter;
        this.recordWriter.openWriter();
        this.bagName = bag;
        loadBagInformation();
	}

    public void closeBagInformation() throws JasDBStorageException {
        this.recordWriter.closeWriter();
    }
    
	public void createOrUpdate(BagPartition partition) throws JasDBStorageException {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            log.debug("Creating or updating partition metadata: {} to bag: {}", partition.toHeader(), bagName);
            String partitionData = MetadataType.PARTITION_DATA.getType() + partition.toHeader();
            if(!bagPartitions.containsKey(partition.getPartitionId())) {
                String documentId = writeRecord(partitionData);

                this.bagPartitions.put(partition.getPartitionId(), new BagPartitionRecordWrapper(partition, documentId));
            } else {
                BagPartitionRecordWrapper wrapper = bagPartitions.get(partition.getPartitionId());
                recordWriter.updateRecord(new UUIDKey(wrapper.getId()), RecordStreamUtil.toStream(partitionData));
                recordWriter.flush();
                this.bagPartitions.put(partition.getPartitionId(), new BagPartitionRecordWrapper(partition, wrapper.getId()));
            }
        } finally {
            writeLock.unlock();
        }
	}

    private String writeRecord(String data) throws JasDBStorageException {
        UUID randomId = UUID.randomUUID();
        UUIDKey idKey = new UUIDKey(randomId);
        recordWriter.writeRecord(idKey, RecordStreamUtil.toStream(data));
        recordWriter.flush();

        return randomId.toString();
    }

	public void addIndex(IndexDefinition index) throws JasDBStorageException {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            log.debug("Adding index metadata: {} to bag: {}", index.toHeader(), bagName);
            if(!indexDefinitions.containsKey(index.getIndexName())) {
                String documentId = writeRecord(MetadataType.INDEX_DATA.getType() + index.toHeader());
                this.indexDefinitions.put(index.getIndexName(), new IndexDefinitionWrapper(index, documentId));
            }
        } finally {
            writeLock.unlock();
        }
	}

    public void removeIndex(IndexDefinition index) throws JasDBStorageException {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        try {
            log.debug("Removing index metadata: {} from bag: {}", index.toHeader(), bagName);
            if(indexDefinitions.containsKey(index.getIndexName())) {
                IndexDefinitionWrapper wrapper = indexDefinitions.get(index.getIndexName());

                log.debug("Removing: {}", wrapper.getDocumentId());
                recordWriter.removeRecord(new UUIDKey(wrapper.getDocumentId()));
                recordWriter.flush();
                this.indexDefinitions.remove(index.getIndexName());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsPartition(BagPartition partition) throws JasDBStorageException {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return bagPartitions.containsKey(partition.getPartitionId());
        } finally {
            readLock.unlock();
        }
    }

    public boolean containsIndex(IndexDefinition index) throws JasDBStorageException {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return indexDefinitions.containsKey(index.getIndexName());
        } finally {
            readLock.unlock();
        }
    }
	
	public Set<IndexDefinition> getIndexes() {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
		    Set<IndexDefinition> definitions = new HashSet<IndexDefinition>(indexDefinitions.size());
            for(IndexDefinitionWrapper wrapper : indexDefinitions.values()) {
                definitions.add(wrapper.getDefinition());
            }
            return definitions;
        } finally {
            readLock.unlock();
        }
	}
	
	public Set<BagPartition> getPartitions() {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            Collection<BagPartitionRecordWrapper> wrappers = bagPartitions.values();
            Set<BagPartition> partitions = new HashSet<BagPartition>(wrappers.size());
            for(BagPartitionRecordWrapper wrapper : wrappers) {
                partitions.add(wrapper.getPartition());
            }
            return partitions;
        } finally {
            readLock.unlock();
        }
	}
    
    @Override
    public BagPartition getPartitionById(String partitionId) {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            BagPartitionRecordWrapper wrapper = bagPartitions.get(partitionId);
            if(wrapper != null) {
                return wrapper.getPartition();
            } else {
                return null;
            }
        } finally {
            readLock.unlock();
        }
    }
    
    public BagPartition getPartitionByRange(String strategy, String start, String end) {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            for(BagPartitionRecordWrapper bpRecord : bagPartitions.values()) {
                BagPartition partition = bpRecord.getPartition();
                if(partition.getPartitionStrategy().equals(strategy) && partition.getStart().equals(start) && partition.getEnd().equals(end)) {
                    return partition;
                }
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    private void loadBagInformation() throws JasDBStorageException {
        for(RecordResult record : recordWriter.readAllRecords()) {
            log.debug("Loading record; {}", record.getId());
            String recordContents = RecordStreamUtil.toString(record);
            MetadataType type = getMetadataType(recordContents);
            if(type != null) {
                String strippedTypeHeader = recordContents.substring(type.getType().length());
                if(type == MetadataType.INDEX_DATA) {
                    IndexDefinition definition = IndexDefinition.fromHeader(strippedTypeHeader);
                    this.indexDefinitions.put(definition.getIndexName(), new IndexDefinitionWrapper(definition, record.getId().getValue()));
                } else if (type == MetadataType.PARTITION_DATA) {
                    BagPartition partition = BagPartition.fromHeader(strippedTypeHeader);
                    this.bagPartitions.put(partition.getPartitionId(), new BagPartitionRecordWrapper(partition, record.getId().getValue()));
                } else {
                    throw new JasDBStorageException("Unrecognized metadata persisted for bag: " + bagName);
                }
            } else {
                throw new JasDBStorageException("Corrupt metadata information persisted for bag: " + bagName);
            }
        }
    }
    
    private MetadataType getMetadataType(String recordContents) {
        for(MetadataType type : MetadataType.values()) {
            if(recordContents.startsWith(type.getType())) {
                return type;
            }
        }

        return null;
    }

    private class IndexDefinitionWrapper {
        private IndexDefinition definition;
        private String documentId;

        public IndexDefinitionWrapper(IndexDefinition definition, String documentId) {
            this.definition = definition;
            this.documentId = documentId;
        }

        public IndexDefinition getDefinition() {
            return definition;
        }

        public String getDocumentId() {
            return documentId;
        }
    }
    
    private class BagPartitionRecordWrapper {
        private BagPartition partition;
        private String id;
        
        private BagPartitionRecordWrapper(BagPartition partition, String id) {
            this.partition = partition;
            this.id = id;
        }

        public BagPartition getPartition() {
            return partition;
        }

        public String getId() {
            return id;
        }
    }
}
