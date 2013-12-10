package nl.renarj.jasdb.storage.indexing;

import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Component
public class IndexManagerFactoryImpl implements IndexManagerFactory {

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
}
