package nl.renarj.jasdb.service.search;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapperImpl;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.index.search.SearchCondition;
import nl.renarj.jasdb.storage.query.operators.BlockMerger;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;
import org.slf4j.Logger;

import java.util.*;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
public class TableScanOperation {
    private static final Logger LOG = getLogger(TableScanOperation.class);

    private final RecordWriter recordWriter;

    public TableScanOperation(RecordWriter recordWriter) {
        this.recordWriter = recordWriter;
    }

    public IndexSearchResultIteratorCollection doTableScan(BlockOperation operation, Set<String> fields, IndexSearchResultIteratorCollection currentResults) throws JasDBStorageException {
        BlockMerger merger = operation.getMerger();
        List<Key> foundKeys = new ArrayList<>();

        Set<String> payloadFields = new HashSet<>(fields);
        payloadFields.add(SimpleEntity.DOCUMENT_ID);

        for(Iterator<String> fieldIterator = payloadFields.iterator(); fieldIterator.hasNext();) {
            String field = fieldIterator.next();
            if(!operation.hasConditions(field)) {
                fieldIterator.remove();
            }
        }

        KeyNameMapperImpl keyNameMapper = new KeyNameMapperImpl();
        keyNameMapper.addMappedField(0, SimpleEntity.DOCUMENT_ID);
        if(currentResults != null) {
            LOG.info("Doing table scan for fields: {} with limited set: {}", fields, currentResults.size());
            for(Key key : currentResults) {
                RecordResult result = recordWriter.readRecord(KeyUtil.getDocumentKey(currentResults.getKeyNameMapper(), key));
                doTableScanConditions(operation, merger, foundKeys, result, fields, keyNameMapper);
            }
        } else {
            LOG.info("Doing a full table scan for fields: {}", fields);
            RecordIterator recordIterator = recordWriter.readAllRecords();

            for(RecordResult result : recordIterator) {
                doTableScanConditions(operation, merger, foundKeys, result, payloadFields, keyNameMapper);
            }
        }

        return new IndexSearchResultIteratorImpl(foundKeys, keyNameMapper);
    }

    private void doTableScanConditions(BlockOperation operation, BlockMerger merger, List<Key> foundKeys, RecordResult result, Set<String> fields, KeyNameMapper keyNameMapper) throws JasDBStorageException {
        SimpleEntity entity = SimpleEntity.fromStream(result.getStream());

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
            CompositeKey compositeKey = new CompositeKey();
            compositeKey.addKey(keyNameMapper, SimpleEntity.DOCUMENT_ID, new UUIDKey(entity.getInternalId()));
            foundKeys.add(compositeKey);
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
