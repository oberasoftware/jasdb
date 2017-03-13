package com.oberasoftware.jasdb.engine.indexing;

import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
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
