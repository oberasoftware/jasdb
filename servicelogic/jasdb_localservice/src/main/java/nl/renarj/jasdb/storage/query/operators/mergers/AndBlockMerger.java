/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.query.operators.mergers;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.storage.query.operators.BlockMerger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AndBlockMerger implements BlockMerger {

	@Override
	public boolean includeResult(boolean leftResultFound, boolean rightResultFound) {
		return leftResultFound && rightResultFound;
	}

    @Override
    public boolean continueEvaluation(boolean currentState) {
        return currentState;
    }

    @Override
	public IndexSearchResultIteratorCollection mergeIterators(IndexSearchResultIteratorCollection mergeInto, IndexSearchResultIteratorCollection... results) throws JasDBStorageException {
        List<Key> mergeIntoKeys = new LinkedList<>();
        for(Key mergeKey : mergeInto.getKeys()) {
            mergeIntoKeys.add(mergeKey.cloneKey(true));
        }

        KeyNameMapper keyNameMapper = mergeInto.getKeyNameMapper();

		for(IndexSearchResultIteratorCollection collection : results) {
			mergeCollection(collection, mergeIntoKeys, keyNameMapper);
		}
		
		return new IndexSearchResultIteratorImpl(mergeIntoKeys, keyNameMapper);
	}

	private void mergeCollection(IndexSearchResultIteratorCollection collection, List<Key> mergeIntoKeys, KeyNameMapper keyNameMapper) throws JasDBStorageException {
		Map<Key, Key> collectionKeys = new HashMap<>(mergeIntoKeys.size());
		for(Key collectionKey : collection) {
			collectionKeys.put(KeyUtil.getDocumentKey(collection.getKeyNameMapper(), collectionKey), collectionKey);
		}

		for(Iterator<Key> mergeIterator = mergeIntoKeys.iterator(); mergeIterator.hasNext(); ) {
			Key key = mergeIterator.next();
            Key documentKey = KeyUtil.getDocumentKey(keyNameMapper, key);
            if(!collectionKeys.containsKey(documentKey)) {
                mergeIterator.remove();
            } else {
                mergeKeys(key, documentKey, collection.getKeyNameMapper(), keyNameMapper);
            }
		}
	}
	
	private void mergeKeys(Key mergeInto, Key mergeFrom, KeyNameMapper sourceNameMapper, KeyNameMapper targetNameMapper) {
        Set<String> sourceFields = sourceNameMapper.getFieldSet();
        sourceFields.removeAll(targetNameMapper.getFieldSet());

        if(!sourceFields.isEmpty()) {
            for(String sourceField : sourceFields) {
                targetNameMapper.addMappedField(sourceField);

                mergeInto.addKey(targetNameMapper, sourceField, mergeFrom);
            }
        }
	}
}
