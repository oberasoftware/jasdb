package nl.renarj.jasdb.storage.indexing;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexScanReport;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.ScanIntent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                IndexScanReport report = index.scan(ScanIntent.DETECT_INCOMPLETE, new IndexItemIterator(recordIterator));
                if(report.getState() == IndexState.INVALID) {
                    LOG.info("Index: {} state is invalid, completeness: {}, starting rebuild", index, report.getCompleteness());
                    index.rebuildIndex(new IndexItemIterator(recordIterator));
                    LOG.info("Index: {} rebuild completed", index);
                } else {
                    LOG.info("Index: {} scan completed, state ok", index);
                }
            } else {
                LOG.info("Doing index: {} rebuild", index);
                index.rebuildIndex(new IndexItemIterator(recordIterator));
                LOG.info("Index: {} rebuild completed", index);
            }
        } catch(JasDBStorageException e) {
            LOG.error("Unable to rebuild the index", e);
        }
    }

}
