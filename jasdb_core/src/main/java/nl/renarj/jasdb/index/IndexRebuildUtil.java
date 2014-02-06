package nl.renarj.jasdb.index;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
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
                if(KeyUtil.isDataPresent(indexableItem, index)) {
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
