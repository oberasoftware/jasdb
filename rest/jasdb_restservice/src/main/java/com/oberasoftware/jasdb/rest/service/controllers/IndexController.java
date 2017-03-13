package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.engine.IndexManager;
import com.oberasoftware.jasdb.api.engine.IndexManagerFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.core.index.query.SimpleCompositeIndexField;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.rest.model.IndexCollection;
import com.oberasoftware.jasdb.rest.model.IndexEntry;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.oberasoftware.jasdb.rest.service.controllers.ControllerUtil.response;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

/**
 * @author Renze de Vries
 * Date: 3-6-12
 * Time: 16:38
 */
@RestController
public class IndexController {
    private static final Logger LOG = getLogger(IndexController.class);

    private static final int SINGLE_KEY_FIELD = 1;

    private final IndexManagerFactory indexManagerFactory;

    private final StorageServiceFactory storageServiceFactory;

    private final DBInstanceFactory dbInstanceFactory;

    @Autowired
    public IndexController(IndexManagerFactory indexManagerFactory, StorageServiceFactory storageServiceFactory, DBInstanceFactory dbInstanceFactory) {
        this.indexManagerFactory = indexManagerFactory;
        this.storageServiceFactory = storageServiceFactory;
        this.dbInstanceFactory = dbInstanceFactory;
    }


    @RequestMapping(value = "/Bags({bagName})/Indexes", produces = "application/json", method = GET)
    public RestEntity getIndexes(@PathVariable String bagName) throws JasDBException {
        return loadIndexes(null, bagName);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Indexes", produces = "application/json", method = GET)
    public RestEntity getIndexes(@PathVariable String instanceId, @PathVariable String bagName) throws JasDBException {
        return loadIndexes(instanceId, bagName);
    }

    private IndexManager getIndexManager(String instanceId) throws JasDBException {
        DBInstance instance = ControllerUtil.getInstance(dbInstanceFactory, instanceId);

        return indexManagerFactory.getIndexManager(instance.getInstanceId());
    }

    private RestEntity loadIndexes(String instanceId, String bagName) throws JasDBException {
        IndexManager indexManager = getIndexManager(instanceId);
        StatRecord getIndexCounter = StatisticsMonitor.createRecord("getIndexes");
        Map<String, Index> indexes = indexManager.getIndexes(bagName);
        getIndexCounter.stop();
        List<IndexEntry> indexEntries = new ArrayList<>(indexes.size());
        for(Index index : indexes.values()) {
            KeyInfo keyInfo = index.getKeyInfo();
            IndexEntry entry = new IndexEntry(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.hasUniqueConstraint(), index.getIndexType());
            entry.setMemorySize(index.getMemoryManager().getTotalMemoryUsage());
            indexEntries.add(entry);
        }
        return new IndexCollection(indexEntries);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Indexes", method = POST, consumes = "application/json", produces = "application/json")
    public RestEntity writeEntry(@PathVariable String instanceId, @PathVariable String bagName, @RequestBody IndexEntry indexEntry) throws RestException {
        if(StringUtils.hasText(instanceId) && StringUtils.hasText(bagName)) {
            try {
                StorageService storageService = storageServiceFactory.getStorageService(instanceId, bagName);

                KeyInfo keyInfo = new KeyInfoImpl(indexEntry.getKeyHeader(), indexEntry.getValueHeader());
                List<IndexField> indexFields = keyInfo.getIndexKeyFields();
                List<IndexField> indexFieldValues = keyInfo.getIndexValueFields();
                IndexField[] indexFieldValueArray = indexFieldValues.toArray(new IndexField[indexFieldValues.size()]);

                if (indexFields.size() > SINGLE_KEY_FIELD) {
                    CompositeIndexField compositeIndexField = new SimpleCompositeIndexField(indexFields.toArray(new IndexField[indexFields.size()]));
                    storageService.ensureIndex(compositeIndexField, indexEntry.isUniqueConstraint(), indexFieldValueArray);
                } else if (indexFields.size() == SINGLE_KEY_FIELD) {
                    storageService.ensureIndex(indexFields.get(0), indexEntry.isUniqueConstraint(), indexFieldValueArray);
                } else {
                    throw new RestException("Unable to create index, no key fields specified");
                }

                return indexEntry;
            } catch (JasDBStorageException e) {
                throw new RestException("Unable to write index data: " + e.getMessage());
            }
        } else {
            throw new RestException("No InstanceId or Bag Name where specified when creating index");
        }
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Indexes({indexName})", method = DELETE, consumes = "application/json", produces = "application/json")
    public ResponseEntity<?> removeEntry(@PathVariable String instanceId, @PathVariable String bagName, @PathVariable String indexName) throws RestException {
        if(StringUtils.hasText(instanceId) && StringUtils.hasText(bagName)) {
            LOG.debug("Removing index from instance: {} bag: {} with name: {}", instanceId, bagName, indexName);
            try {
                StorageService storageService = storageServiceFactory.getStorageService(instanceId, bagName);
                storageService.removeIndex(indexName);

                return response(null, HttpStatus.NO_CONTENT);
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to remove index data: " + e.getMessage());
            }
        } else {
            throw new RestException("Cannot remove index without a specified bag or InstanceId");
        }
    }

}