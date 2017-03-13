package com.oberasoftware.jasdb.core.index;

import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public class IndexRebuildUtil {
    private static final int REPORT_INTERVAL = 100000;
    private static final Logger LOG = LoggerFactory.getLogger(IndexRebuildUtil.class);

    public static void bulkInsertIndex(Index index, KeyInfo keyInfo, Iterator<IndexableItem> indexableItems) throws JasDBStorageException {
        KeyFactory keyFactory = keyInfo.getKeyFactory();
        long counter = 0;
        int interval = 0;
        long start = System.currentTimeMillis();
        while(indexableItems.hasNext()) {
            IndexableItem indexableItem = indexableItems.next();
            if(keyFactory.isMultiValueKey(indexableItem)) {
                Set<Key> keys = keyFactory.createMultivalueKeys(indexableItem);
                for(Key key : keys) {
                    index.insertIntoIndex(key);
                }
            } else {
                if(KeyUtil.isAnyDataPresent(indexableItem, index)) {
                    Key key = keyFactory.createKey(indexableItem);
                    index.insertIntoIndex(key);
                }
            }
            interval++;
            counter++;
            if(interval >= REPORT_INTERVAL) {
                LOG.info("Index: {} rebuild at: {} items", index, counter);
                interval = 0;
            }
        }
        long end = System.currentTimeMillis();
        LOG.info("Finished rebuild for: {} items in: {}", counter, (end - start));
    }
}
