package com.oberasoftware.jasdb.core.index;

import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.IndexScanReport;
import com.oberasoftware.jasdb.api.index.IndexState;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
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
                    return new IndexScanReportImpl(IndexState.INVALID, System.currentTimeMillis(), 0);
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
        return new IndexScanReportImpl(state, System.currentTimeMillis(), completeness);
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
