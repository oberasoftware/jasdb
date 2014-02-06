package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.TokenType;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.input.conditions.InputCondition;
import nl.renarj.jasdb.rest.model.IndexCollection;
import nl.renarj.jasdb.rest.model.IndexEntry;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 * Date: 3-6-12
 * Time: 16:38
 */
@Component
public class IndexModelLoader extends AbstractModelLoader{
    private static final int SINGLE_KEY_FIELD = 1;
    private Logger log = LoggerFactory.getLogger(IndexModelLoader.class);

    @Inject
    private IndexManagerFactory indexManagerFactory;

    @Inject
    private StorageServiceFactory storageServiceFactory;

    @Override
    public String[] getModelNames() {
        return new String[] {"Indexes"};
    }

    @Override
    public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();

            try {
                IndexManager indexManager = indexManagerFactory.getIndexManager(bag.getInstanceId());

                StatRecord getIndexCounter = StatisticsMonitor.createRecord("getIndexes");
                Map<String, Index> indexes = indexManager.getIndexes(bag.getName());
                getIndexCounter.stop();
                List<IndexEntry> indexEntries = new ArrayList<>(indexes.size());
                for(Index index : indexes.values()) {
                    KeyInfo keyInfo = index.getKeyInfo();
                    IndexEntry entry = new IndexEntry(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.hasUniqueConstraint(), index.getIndexType());
                    entry.setMemorySize(index.getMemoryManager().getTotalMemoryUsage());
                    indexEntries.add(entry);
                }
                return new IndexCollection(indexEntries);
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to load index data: " + e.getMessage());
            }
        } else {
            throw new RestException("Cannot retrieve index information without a specified bag");
        }
    }

    @Override
    public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();
            log.debug("Raw entity data received: {}", rawData);
            IndexEntry indexEntry = serializer.deserialize(IndexEntry.class, rawData);

            try {
                StorageService storageService = storageServiceFactory.getStorageService(bag.getInstanceId(), bag.getName());

                KeyInfo keyInfo = new KeyInfoImpl(indexEntry.getKeyHeader(), indexEntry.getValueHeader());
                List<IndexField> indexFields = keyInfo.getIndexKeyFields();
                List<IndexField> indexFieldValues = keyInfo.getIndexValueFields();
                IndexField[] indexFieldValueArray = indexFieldValues.toArray(new IndexField[indexFieldValues.size()]);

                if(indexFields.size() > SINGLE_KEY_FIELD) {
                    CompositeIndexField compositeIndexField = new CompositeIndexField(indexFields.toArray(new IndexField[indexFields.size()]));
                    storageService.ensureIndex(compositeIndexField, indexEntry.isUniqueConstraint(), indexFieldValueArray);
                } else if(indexFields.size() == SINGLE_KEY_FIELD) {
                    storageService.ensureIndex(indexFields.get(0), indexEntry.isUniqueConstraint(), indexFieldValueArray);
                } else {
                    throw new RestException("Unable to create index, no key fields specified");
                }

                return indexEntry;
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to write index data: " + e.getMessage());
            }
        } else {
            throw new RestException("Cannot create index without a specified bag");
        }
    }

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();

            InputCondition condition = input.getCondition();
            if(condition.getTokenType() == TokenType.LITERAL && ((FieldCondition)condition).getField().equals(FieldCondition.ID_PARAM)) {
                FieldCondition idCondition = (FieldCondition) condition;

                try {
                    StorageService storageService = storageServiceFactory.getStorageService(bag.getInstanceId(), bag.getName());
                    storageService.removeIndex(idCondition.getValue());

                    return null;
                } catch(JasDBStorageException e) {
                    throw new RestException("Unable to remove index data: " + e.getMessage());
                }
            } else {
                throw new RestException("Unable to remove index, no index name was specified");
            }
        } else {
            throw new RestException("Cannot remove index without a specified bag");
        }
    }

}