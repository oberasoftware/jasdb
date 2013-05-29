package nl.renarj.jasdb.storage.indexing;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexScanReport;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.ScanIntent;
import nl.renarj.jasdb.service.BagOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Renze de Vries
 */
public class IndexScanAndRecovery implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IndexScanAndRecovery.class);

    private Index index;
    private RecordIterator recordIterator;
    private boolean forceRebuild = false;

    public IndexScanAndRecovery(Index index, RecordIterator recordIterator) {
        this.index = index;
        this.recordIterator = recordIterator;
    }

    public IndexScanAndRecovery(Index index, RecordIterator recordIterator, boolean forceRebuild) {
        this.index = index;
        this.recordIterator = recordIterator;
        this.forceRebuild = forceRebuild;
    }

    @Override
    public void run() {
        try {
            if(!forceRebuild) {
                LOG.info("Doing scan of index: {}", index);
                IndexScanReport report = index.scan(ScanIntent.DETECT_INCOMPLETE, new IndexItemIterator());
                if(report.getState() == IndexState.INVALID) {
                    LOG.info("Index: {} state is invalid, completeness: {}, starting rebuild", index, report.getCompleteness());
                    index.rebuildIndex(new IndexItemIterator());
                    LOG.info("Index: {} rebuild completed", index);
                } else {
                    LOG.info("Index: {} scan completed, state ok", index);
                }
            } else {
                LOG.info("Doing index: {} rebuild", index);
                index.rebuildIndex(new IndexItemIterator());
                LOG.info("Index: {} rebuild completed", index);
            }
        } catch(JasDBStorageException e) {
            LOG.error("Unable to rebuild the index", e);
        }
    }

    private class IndexItemIterator implements Iterator<IndexableItem> {
        public IndexItemIterator() {
            recordIterator.reset();
        }

        @Override
        public boolean hasNext() {
            return recordIterator.hasNext();
        }

        @Override
        public IndexableItem next() {
            RecordResult result = recordIterator.next();
            try {
                SimpleEntity entity = BagOperationUtil.toEntity(result.getStream());
                return entity;
            } catch(JasDBStorageException e) {
                LOG.error("Unable to parse entity", e);
                return null;
            }
        }

        @Override
        public void remove() {}
    }
}
