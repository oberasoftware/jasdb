/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 *
 * All the code and design principals in the codebase are also Copyright 2011
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.indexing;

import com.google.common.collect.Lists;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.btreeplus.BTreeIndex;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final public class IndexManagerImpl implements IndexManager {
	private Logger log = LoggerFactory.getLogger(IndexManagerImpl.class);

	private static final String INDEX_CONFIG_XPATH = "/jasdb/Index";
    private static final String BAG_INDEX_NAME_SPLITTER = "_";
	private static final String INDEX_EXTENSION = ".idx";
	private static final String INDEX_EXTENSION_MULTI = ".idxm";

	private Map<String, Map<String, Index>> indexes = new ConcurrentHashMap<String, Map<String, Index>>();

    private Instance instance;

	private Configuration configuration;
    private MetadataStore metadataStore;

	public IndexManagerImpl(MetadataStore metadataStore, Instance instance, Configuration configuration) {
        this.instance = instance;
		this.configuration = configuration;
        this.metadataStore = metadataStore;
	}

	@Override
	public void shutdownIndexes() throws JasDBStorageException {
		log.info("Shutting down {} indexes", indexes.size());
		for(String bagName : indexes.keySet()) {
			for(Index index : indexes.get(bagName).values()) {
				log.debug("Closing index: {} on bag: {}", index.getKeyInfo().getKeyName(), bagName);
				index.closeIndex();
			}
		}
		indexes.clear();
	}

	@Override
	public List<Index> getLoadedIndexes() {
		List<Index> loadedIndexes = new ArrayList<Index>();
		Collection<Map<String, Index>> allBagIndexes = indexes.values();
		for(Map<String, Index> bagIndexes : allBagIndexes) {
			loadedIndexes.addAll(bagIndexes.values());
		}

		return loadedIndexes;
	}

	@Override
	public Index getBestMatchingIndex(String bagName, Set<String> fields) throws JasDBStorageException {
		Collection<Index> indexes = getIndexes(bagName).values();
		int bestMatch = 0;
		Index currentBestMatch = null;
		for(Index index : indexes) {
			int match = index.match(fields);
			if(match > 0 && match > bestMatch) {
				bestMatch = match;
				currentBestMatch = index;
			}
		}

		return currentBestMatch;
	}

	@Override
	public Map<String, Index> getIndexes(String bagName) throws JasDBStorageException {
		log.debug("Loading indexes for bag: {}", bagName);
		if(!indexes.containsKey(bagName)) {
			log.debug("Indexes where not loaded or not present yet for bag: {}, attempt load", bagName);
			loadIndexes(bagName);
		}

		if(indexes.containsKey(bagName)) {
			log.debug("Indexes are present and loaded returning for bag: {}", bagName);
            return new HashMap<String, Index>(indexes.get(bagName));
		} else {
			throw new JasDBStorageException("No indexes found for bag: " + bagName);
		}
	}

	@Override
	public Index getIndex(String bagName, String keyName) throws JasDBStorageException {
		log.debug("Loading indexes for bag: {} and key: {}", bagName, keyName);
		if(!indexes.containsKey(bagName)) {
			log.debug("Indexes where not loaded or not present yet for bag: {}, attempt load", bagName);
			loadIndexes(bagName);
		}

		if(indexes.containsKey(bagName)) {
			Map<String, Index> bagIndexes = indexes.get(bagName);
			if(bagIndexes.containsKey(keyName)) {
				log.debug("Index found for key: {} in bag: {}", keyName, bagName);
				return bagIndexes.get(keyName);
			} else {
				throw new JasDBStorageException("No index found for key: " + keyName + " in bag: " + bagName);
			}
		} else {
			throw new JasDBStorageException("No indexes found for bag: " + bagName);
		}
	}

    @Override
    public void removeIndex(String bagName, String keyName) throws JasDBStorageException {
        Index index = getIndex(bagName, keyName);
        Map<String, Index> bagIndexes = indexes.get(bagName);
        bagIndexes.remove(keyName);

        index.removeIndex();
    }

    @Override
	public Index createIndex(String bagName, CompositeIndexField compositeIndexFields, boolean unique, IndexField... values) throws JasDBStorageException {
        KeyInfo keyInfo;
        if(unique) {
            keyInfo = new KeyInfoImpl(compositeIndexFields.getIndexFields(), guaranteeIdKey(values));
        } else {
            List<IndexField> indexFields = Lists.newArrayList(compositeIndexFields.getIndexFields());
            indexFields.add(new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType()));
            keyInfo = new KeyInfoImpl(indexFields, Lists.newArrayList(values));
        }

		return createInStore(bagName, keyInfo);
	}

	@Override
	public Index createIndex(String bagName, IndexField indexField, boolean unique, IndexField... valueFields) throws JasDBStorageException {
        KeyInfo keyInfo;
        if(unique) {
            keyInfo = new KeyInfoImpl(indexField, guaranteeIdKey(valueFields));
        } else {
            keyInfo = new KeyInfoImpl(Lists.newArrayList(indexField, new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType())), Lists.newArrayList(valueFields));
        }

		return createInStore(bagName, keyInfo);
	}

    private Index createInStore(String bagName, KeyInfo keyInfo) throws JasDBStorageException {
		if(!indexes.containsKey(bagName)) {
			loadIndexes(bagName);
		}

		Map<String, Index> bagIndexes = this.indexes.get(bagName);
		if(bagIndexes != null && !bagIndexes.containsKey(keyInfo.getKeyName())) {
            File indexFile = createIndexFile(bagName, keyInfo.getKeyName(), false);

			try {
				Index index = new BTreeIndex(indexFile, keyInfo);
				configureIndex(IndexTypes.BTREE, index);

				bagIndexes.put(keyInfo.getKeyName(), index);
				return index;
			} catch(ConfigurationException e) {
				throw new JasDBStorageException("Unable to create index, configuration error", e);
			}
		} else if(bagIndexes != null){
			return bagIndexes.get(keyInfo.getKeyName());
		} else {
			return null;
		}

	}

	private List<IndexField> guaranteeIdKey(IndexField... valueFields) throws JasDBStorageException {
		Set<String> fields = new HashSet<String>();
		List<IndexField> indexFields = new ArrayList<IndexField>();
		for(IndexField valueField : valueFields) {
			fields.add(valueField.getField());
			indexFields.add(valueField);
		}

		if(!fields.contains(SimpleEntity.DOCUMENT_ID)) {
			indexFields.add(new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType()));
		}

		return indexFields;
	}

	private synchronized void loadIndexes(final String bagName) throws JasDBStorageException {
		log.debug("Loading indexes for bag: {}", bagName);
		if(!indexes.containsKey(bagName)) {
            Bag bag = metadataStore.getBag(instance.getInstanceId(), bagName);
            if(bag != null) {
                Set<IndexDefinition> indexDefinitions = new HashSet<IndexDefinition>(bag.getIndexDefinitions());

                log.info("Found {} potential indexes for bag: {}", indexDefinitions.size(), bagName);
                Map<String, Index> bagIndexes = new HashMap<String, Index>();
                for(IndexDefinition indexDefinition : indexDefinitions) {
                    Index index = loadIndex(bagName, indexDefinition);
                    bagIndexes.put(index.getKeyInfo().getKeyName(), index);
                }
                this.indexes.put(bagName, bagIndexes);
            }
		}
	}

	private Index loadIndex(String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
		try {
            KeyInfo keyInfo = new KeyInfoImpl(indexDefinition.getHeaderDescriptor(), indexDefinition.getValueDescriptor());
            File indexFile = createIndexFile(bagName, indexDefinition.getIndexName(), false);

            switch(IndexTypes.getTypeFor(indexDefinition.getIndexType())) {
                case BTREE:
                    log.debug("Loaded BTree Index for key: {}", indexDefinition.getIndexName());
                    Index btreeIndex = new BTreeIndex(indexFile, keyInfo);

                    return configureIndex(IndexTypes.BTREE, btreeIndex);
                default:
                    throw new JasDBStorageException("Reading from this index type: " + indexDefinition.getIndexName() +
                            " is not supported");
            }
        } catch(ConfigurationException e) {
			throw new JasDBStorageException("Unable to load index, invalid configuration", e);
		}
	}

	private Index configureIndex(IndexTypes indexType, Index indexPersister) throws ConfigurationException {
		Configuration indexConfig = this.configuration.getChildConfiguration(INDEX_CONFIG_XPATH + "[@Type='" + indexType.getName() + "']");

		if(indexConfig != null) {
			log.info("Using configuration for index type: {}", indexType.getName());
			indexPersister.configure(indexConfig);
		} else {
			log.info("There is no configuration for index type: {} using defaults", indexType.getName());
		}
		return indexPersister;
	}
    
    private File createIndexFile(String bagName, String indexName, boolean multi) {
        StringBuilder indexFileNameBuilder = new StringBuilder();
        String extension = multi ? INDEX_EXTENSION_MULTI : INDEX_EXTENSION;

        indexFileNameBuilder.append(bagName).append(BAG_INDEX_NAME_SPLITTER);
        indexFileNameBuilder.append(indexName).append(extension);

        return new File(instance.getPath(), indexFileNameBuilder.toString());
    }
}
