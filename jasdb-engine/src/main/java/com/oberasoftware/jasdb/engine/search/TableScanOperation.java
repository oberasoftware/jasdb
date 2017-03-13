package com.oberasoftware.jasdb.engine.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.Property;
import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;
import com.oberasoftware.jasdb.engine.query.operators.BlockMerger;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.oberasoftware.jasdb.engine.BagOperationUtil.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
public class TableScanOperation {
    private static final Logger LOG = getLogger(TableScanOperation.class);

    private final RecordWriter<UUIDKey> recordWriter;

    public TableScanOperation(RecordWriter<UUIDKey> recordWriter) {
        this.recordWriter = recordWriter;
    }

    public IndexSearchResultIteratorCollection doTableScanFindAll() throws JasDBStorageException {
        List<Key> keys = new ArrayList<>();
        RecordIterator recordIterator = recordWriter.readAllRecords();
        recordIterator.forEach(r -> keys.add(recordToKey(r)));

        return new IndexSearchResultIteratorImpl(keys, DEFAULT_DOC_ID_MAPPER.clone());
    }

    public IndexSearchResultIteratorCollection doTableScan(BlockOperation operation, Set<String> fields, IndexSearchResultIteratorCollection currentResults) throws JasDBStorageException {
        BlockMerger merger = operation.getMerger();
        List<Key> foundKeys = new ArrayList<>();

        Set<String> payloadFields = new HashSet<>(fields);
        payloadFields.add(SimpleEntity.DOCUMENT_ID);

        payloadFields.removeIf(field -> !operation.hasConditions(field));

        if(currentResults != null) {
            LOG.debug("Doing table scan for fields: {} with limited set: {}", fields, currentResults.size());
            for(Key key : currentResults) {
                RecordResult result = recordWriter.readRecord(KeyUtil.getDocumentKey(currentResults.getKeyNameMapper(), key));
                doTableScanConditions(operation, merger, foundKeys, result, fields);
            }
        } else {
            LOG.debug("Doing a full table scan for fields: {}", fields);
            RecordIterator recordIterator = recordWriter.readAllRecords();

            for(RecordResult result : recordIterator) {
                doTableScanConditions(operation, merger, foundKeys, result, payloadFields);
            }
        }

        return new IndexSearchResultIteratorImpl(foundKeys, DEFAULT_DOC_ID_MAPPER.clone());
    }

    private void doTableScanConditions(BlockOperation operation, BlockMerger merger, List<Key> foundKeys, RecordResult result, Set<String> fields) throws JasDBStorageException {
        Entity entity = SimpleEntity.fromStream(result.getStream());

        boolean first = true;
        boolean match = false;

        for(String field : fields) {
            Property property = entity.getProperty(field);
            if(property != null) {
                Set<Key> keys = PropertyKeyMapper.mapToKeys(property);

                Set<SearchCondition> conditions = operation.getConditions(field);
                for(SearchCondition condition : conditions) {
                    boolean newmatch = checkCondition(condition, keys);

                    match = first && newmatch || merger.includeResult(match, newmatch);

                    first = false;

                    if (!merger.continueEvaluation(match)) {
                        return;
                    }
                }
            } else if(!merger.continueEvaluation(false)) {
                //in this case when no property we should stop evaluating
                return;
            }
        }

        if(match) {
            foundKeys.add(entityToKey(entity));
        }
    }


    private boolean checkCondition(SearchCondition condition, Set<Key> keys) {
        for(Key key : keys) {
            boolean match = condition.keyQualifies(key);

            if(match) return true;
        }
        return false;
    }
}
