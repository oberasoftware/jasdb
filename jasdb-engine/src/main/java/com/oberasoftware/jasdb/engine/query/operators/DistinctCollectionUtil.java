package com.oberasoftware.jasdb.engine.query.operators;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public class DistinctCollectionUtil {
    public static IndexSearchResultIteratorCollection distinct(IndexSearchResultIteratorCollection collection) throws JasDBStorageException {
        List<Key> keys = collection.getKeys();
        Set<Key> documentKeys = new HashSet<>();
        for(Iterator<Key> distinctIterator = keys.iterator(); distinctIterator.hasNext(); ) {
            Key documentKey = KeyUtil.getDocumentKey(collection.getKeyNameMapper(), distinctIterator.next());
            if(!documentKeys.contains(documentKey)) {
                documentKeys.add(documentKey);
            } else {
                distinctIterator.remove();
            }
        }
        return new IndexSearchResultIteratorImpl(keys, collection.getKeyNameMapper());
    }


}
