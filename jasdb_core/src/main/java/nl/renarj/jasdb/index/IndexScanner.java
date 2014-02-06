package nl.renarj.jasdb.index;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.search.EqualsCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public class IndexScanner {
    private static final int REPORT_INTERVAL = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(IndexScanner.class);

    public static IndexScanReport doIndexScan(Index index, KeyInfo keyInfo, Iterator<IndexableItem> indexableItems, boolean fullScan) throws JasDBStorageException {
        KeyFactory keyFactory = keyInfo.getKeyFactory();
        long indexedItems = 0;
        long expectedItems = 0;

        int interval = 0;
        while(indexableItems.hasNext()) {
            IndexableItem indexableItem = indexableItems.next();

            if(isIndexable(keyInfo, indexableItem)) {
                boolean isInIndex = true;
                if(keyFactory.isMultiValueKey(indexableItem)) {
                    Set<Key> keys = keyFactory.createMultivalueKeys(indexableItem);
                    for(Key key : keys) {
                        if(index.searchIndex(new EqualsCondition(key), Index.NO_SEARCH_LIMIT).isEmpty()) {
                            isInIndex = false;
                            break;
                        }
                    }
                } else {
                    isInIndex = !index.searchIndex(new EqualsCondition(keyFactory.createKey(indexableItem)), Index.NO_SEARCH_LIMIT).isEmpty();
                }

                expectedItems++;
                if(isInIndex) {
                    indexedItems++;
                } else if(!fullScan) {
                    return new IndexScanReport(IndexState.INVALID, System.currentTimeMillis(), 0);
                }
            }

            interval++;
            if(interval >= REPORT_INTERVAL) {
                LOG.info("Index: {} scan at: {} items", index, indexedItems);
                interval = 0;
            }
        }
        IndexState state = indexedItems == expectedItems ? IndexState.OK : IndexState.INVALID;
        LOG.info("Completed index scan found: {} in index", indexedItems);
        int completeness = (int)(((double)indexedItems / (double)expectedItems) * 100);
        return new IndexScanReport(state, System.currentTimeMillis(), completeness);
    }

    private static boolean isIndexable(KeyInfo keyInfo, IndexableItem item) {
        for(String field : keyInfo.getKeyFields()) {
            if(!item.hasValue(field)) {
                return false;
            }
        }
        return true;
    }


}
