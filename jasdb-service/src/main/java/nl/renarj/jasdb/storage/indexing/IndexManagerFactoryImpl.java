package nl.renarj.jasdb.storage.indexing;

import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Component
public class IndexManagerFactoryImpl implements IndexManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(IndexManagerFactoryImpl.class);

    private ConcurrentHashMap<String, IndexManager> indexManagers = new ConcurrentHashMap<>();

    @Inject
    private MetadataStore metadataStore;

    @Inject
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
