package nl.renarj.jasdb.storage.query.operators;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;

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
        Set<Key> documentKeys = new HashSet<Key>();
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
