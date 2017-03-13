package com.oberasoftware.jasdb.engine.indexing;

import com.oberasoftware.jasdb.api.engine.IndexManager;
import com.oberasoftware.jasdb.api.engine.IndexManagerFactory;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Component
public class IndexManagerFactoryImpl implements IndexManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(IndexManagerFactoryImpl.class);

    private ConcurrentHashMap<String, IndexManager> indexManagers = new ConcurrentHashMap<>();

    @Autowired
    private MetadataStore metadataStore;

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public IndexManager getIndexManager(String instanceId) throws JasDBStorageException {
        if(!indexManagers.containsKey(instanceId)) {
            if(metadataStore.containsInstance(instanceId)) {
                IndexManager indexManager = (IndexManager) applicationContext.getBean("IndexManager", instanceId);
                indexManagers.putIfAbsent(instanceId, indexManager);
            } else {
                throw new JasDBStorageException("Unable to get index manager, instance does not exist");
            }
        }

        return indexManagers.get(instanceId);
    }

    @Override
    @PreDestroy
    public void shutdownIndexes() throws JasDBStorageException {
        LOG.info("Shutting down indexes");
        for(IndexManager indexManager : indexManagers.values()) {
            indexManager.shutdownIndexes();
        }
        LOG.info("Finished shutting down indexes");
    }
}
