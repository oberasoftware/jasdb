/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.keyinfo;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.factory.CompositeKeyFactory;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;
import nl.renarj.jasdb.index.keys.factory.KeyFactoryManager;
import nl.renarj.jasdb.index.keys.factory.WrappedValueKeyFactory;
import nl.renarj.jasdb.index.search.IndexField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class KeyInfoImpl implements KeyInfo {
	private static final Logger LOG = LoggerFactory.getLogger(KeyInfoImpl.class);

    private KeyFactory wrapperKeyFactory;

	private final KeyFactory keyFactory;

	private final MultiKeyloader multiValueKeyLoader;
	private List<String> fields;
    private List<String> valueFields;
    private KeyNameMapper keyNameMapper;

    public KeyInfoImpl(IndexField indexField, IndexField... valueFields) throws JasDBStorageException {
        this(indexField, Arrays.asList(valueFields));
    }

    public KeyInfoImpl(IndexField indexField, List<IndexField> valueIndexFields) throws JasDBStorageException {
        this(Lists.newArrayList(indexField), valueIndexFields);
	}

    public KeyInfoImpl(List<IndexField> keyFields, List<IndexField> valueFields) throws JasDBStorageException {
        if(keyFields.size() == 1) {
            IndexField indexField = keyFields.get(0);
            this.keyFactory = KeyFactoryManager.createKeyFactory(indexField.getField(), indexField.getKeyType());
            this.fields = Lists.newArrayList(keyFactory.getFieldName());
            this.keyNameMapper = new KeyNameMapperImpl();
            this.keyNameMapper.setValueMarker(0);
        } else if(keyFields.size() > 1) {
            MultiKeyloader keyloader = createMultiKeyLoader(keyFields, new KeyNameMapperImpl());
            this.keyNameMapper = keyloader.getKeyNameMapper();
            this.keyNameMapper.setValueMarker(keyFields.size());
            this.keyFactory = new CompositeKeyFactory(keyloader);
            this.fields = keyloader.getFields();
        } else {
            throw new JasDBStorageException("Unable to create key information, no keys specified");
        }

        this.multiValueKeyLoader = createMultiKeyLoader(valueFields, this.keyNameMapper);
        this.valueFields = multiValueKeyLoader.getFields();
        this.wrapperKeyFactory = new WrappedValueKeyFactory(keyFactory, multiValueKeyLoader);
    }

    private MultiKeyloader createMultiKeyLoader(List<IndexField> fields, KeyNameMapper mapper) throws JasDBStorageException {
        KeyFactory[] keyFactories = new KeyFactory[fields.size()];

        for(int i=0; i<fields.size(); i++) {
            IndexField field = fields.get(i);
            keyFactories[i] = KeyFactoryManager.createKeyFactory(field.getField(), field.getKeyType());
            mapper.addMappedField(field.getField());
        }

        return new MultiKeyLoaderImpl(mapper, keyFactories);
    }

	public KeyInfoImpl(String headerDescriptor, String valueDescriptor) throws JasDBStorageException {
		KeyFactory[] keyFactories = KeyFactoryManager.parseHeader(headerDescriptor);
		if(keyFactories.length == 1) {
            this.keyFactory = keyFactories[0];
            LOG.debug("Loaded key index for field: {} and factory: {}", keyFactory.getFieldName(), keyFactory);

			this.fields = new LinkedList<>();
            this.valueFields = new LinkedList<>();
            if(this.keyFactory instanceof CompositeKeyFactory) {
                MultiKeyloader multiKeyloader = ((CompositeKeyFactory)keyFactory).getMultiKeyloader();
                this.fields.addAll(multiKeyloader.getFields());
                this.keyNameMapper = multiKeyloader.getKeyNameMapper();
                this.keyNameMapper.setValueMarker(multiKeyloader.getFields().size());
            } else {
                this.fields.add(keyFactory.getFieldName());
                this.keyNameMapper = new KeyNameMapperImpl();
                this.keyNameMapper.setValueMarker(0);
            }

            KeyFactory[] valueFactories = KeyFactoryManager.parseHeader(valueDescriptor);
			for(int i=0; i<valueFactories.length; i++) {
                String fieldName = valueFactories[i].getFieldName();
                keyNameMapper.addMappedField(fieldName);
                valueFields.add(fieldName);
			}
			this.multiValueKeyLoader = new MultiKeyLoaderImpl(keyNameMapper, valueFactories);
            this.wrapperKeyFactory = new WrappedValueKeyFactory(keyFactory, multiValueKeyLoader);
		} else {
			throw new JasDBStorageException("Unexpected key information in headers");
		}
	}

    @Override
	public int match(Set<String> searchFields) {
		int matches = 0;
		int valueMatches = 0;
		
		/* because we can only match in same order as found in index, 
		 * we break after next fields is no longer a match */
		for(String field : this.fields) {
			if(searchFields.contains(field)) {
				matches++;
			} else {
				break;
			}
		}

		for(String field : valueFields) {
			if(searchFields.contains(field)) {
				valueMatches++;
			}
		}
		
		int possibleMatches = searchFields.size();
		
		double fieldMatchesPerc =  matches > 0 ? (double)matches / possibleMatches : 0.0;
		double valueMatchesPerc =  valueMatches > 0 ? (double)valueMatches / possibleMatches : 0.0;
		
		return (int)((fieldMatchesPerc + valueMatchesPerc) * 100);
	}

	@Override
	public List<String> getKeyFields() {
		return Collections.unmodifiableList(this.fields);
	}

    @Override
    public List<IndexField> getIndexKeyFields() {
        if(keyFactory instanceof CompositeKeyFactory) {
            List<IndexField> valueFields = new ArrayList<>();
            for(KeyFactory vKeyFactory : ((CompositeKeyFactory)keyFactory).getMultiKeyloader().getKeyFactories()) {
                valueFields.add(new IndexField(vKeyFactory.getFieldName(), vKeyFactory.getKeyType()));
            }
            return valueFields;
        } else {
            return Lists.newArrayList(new IndexField(keyFactory.getFieldName(), keyFactory.getKeyType()));
        }
    }

    @Override
    public List<IndexField> getIndexValueFields() {
        List<IndexField> valueFields = new ArrayList<>();
        for(KeyFactory vKeyFactory : multiValueKeyLoader.getKeyFactories()) {
            valueFields.add(new IndexField(vKeyFactory.getFieldName(), vKeyFactory.getKeyType()));
        }
        return valueFields;
    }

    @Override
	public List<String> getValueFields() {
		return Collections.unmodifiableList(valueFields);
	}

	@Override
	public Key loadKey(int curPosition, ByteBuffer byteBuffer)
			throws JasDBStorageException {
		Key loadedKey = keyFactory.loadKey(curPosition, byteBuffer);
		if(loadedKey != null) {
			int valueOffset = curPosition + keyFactory.getKeySize();
			this.multiValueKeyLoader.loadKeys(loadedKey, valueOffset, byteBuffer);
			
			return loadedKey;
		} else {
			return null;
		}
	}

	@Override
	public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer)
			throws JasDBStorageException {
		this.keyFactory.writeKey(key, curPosition, byteBuffer);
		int valueOffset = curPosition + keyFactory.getKeySize();

		this.multiValueKeyLoader.writeKeys(key, valueOffset, byteBuffer);
	}

    @Override
    public KeyLoadResult loadKey(int curPosition, DataBlock dataBlock) throws JasDBStorageException {
        KeyLoadResult loadResult = keyFactory.loadKey(curPosition, dataBlock);
        if(loadResult != null) {
            loadResult = this.multiValueKeyLoader.loadKeys(loadResult.getLoadedKey(), loadResult.getNextOffset(), loadResult.getEndBlock());

            return loadResult;
        } else {
            return null;
        }
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        DataBlock currentBlock = this.keyFactory.writeKey(key, dataBlock);
        return this.multiValueKeyLoader.writeKeys(key, currentBlock);
    }

    @Override
    public KeyNameMapper getKeyNameMapper() {
        return keyNameMapper;
    }

    @Override
	public String keyAsHeader() {
		return keyFactory.asHeader();
	}
	
	@Override
	public String valueAsHeader() {
		return multiValueKeyLoader.asHeader();
	}
	
	@Override
	public String getKeyName() {
		return this.keyFactory.getFieldName();
	}

    @Override
	public KeyFactory getKeyFactory() {
		return this.wrapperKeyFactory;
	}

    @Override
    public MultiKeyloader getMultiKeyloader() {
        return this.multiValueKeyLoader;
    }

    @Override
	public int getKeySize() {
		return keyFactory.getKeySize() + multiValueKeyLoader.getKeySize();
	}

	@Override
	public KeyInfoType getInfoType() {
		return KeyInfoType.SINGLE_KEY;
	}
	
	@Override
	public String toString() {
		return getKeyName();
	}
}
