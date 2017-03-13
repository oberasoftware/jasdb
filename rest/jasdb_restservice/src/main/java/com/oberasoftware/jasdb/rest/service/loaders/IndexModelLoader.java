package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.core.index.query.SimpleCompositeIndexField;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.engine.IndexManager;
import com.oberasoftware.jasdb.api.engine.IndexManagerFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.service.input.InputElement;
import com.oberasoftware.jasdb.rest.service.input.TokenType;
import com.oberasoftware.jasdb.rest.service.input.OrderParam;
import com.oberasoftware.jasdb.rest.service.input.conditions.FieldCondition;
import com.oberasoftware.jasdb.rest.service.input.conditions.InputCondition;
import com.oberasoftware.jasdb.rest.model.IndexCollection;
import com.oberasoftware.jasdb.rest.model.IndexEntry;
import com.oberasoftware.jasdb.rest.model.RestBag;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    @Autowired
    private IndexManagerFactory indexManagerFactory;

    @Autowired
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
                    CompositeIndexField compositeIndexField = new SimpleCompositeIndexField(indexFields.toArray(new IndexField[indexFields.size()]));
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