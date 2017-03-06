package com.oberasoftware.jasdb.engine.indexing;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import com.oberasoftware.jasdb.engine.BagOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Renze de Vries
 */
public class IndexItemIterator implements Iterator<IndexableItem> {
    private static final Logger LOG = LoggerFactory.getLogger(IndexItemIterator.class);


    private RecordIterator recordResults;

    public IndexItemIterator(RecordIterator recordResults) {
        this.recordResults = recordResults;
    }

    @Override
    public boolean hasNext() {
        return recordResults.hasNext();
    }

    @Override
    public IndexableItem next() {
        RecordResult result = recordResults.next();
        try {
            return BagOperationUtil.toEntity(result.getStream());
        } catch (JasDBStorageException e) {
            LOG.error("Unable to parse entity", e);
            return null;
        }
    }

    @Override
    public void remove() {
    }
}
