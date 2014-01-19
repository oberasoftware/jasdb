package com.obera.jasdb.android;

import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author renarj
 */
@Singleton
public class AndroidIndexManagerFactory implements IndexManagerFactory {

    private ConcurrentHashMap<String, IndexManager> indexManagers = new ConcurrentHashMap<>();

    @Inject
    private MetadataStore metadataStore;

    @Inject
    private GuiceIndexManagerFactory guiceIndexManagerFactory;

    @Override
    public IndexManager getIndexManager(String instanceId) throws JasDBStorageException {
        if(!indexManagers.containsKey(instanceId)) {
            if(metadataStore.containsInstance(instanceId)) {
                IndexManager indexManager = guiceIndexManagerFactory.getIndexManager(instanceId);
//                IndexManager indexManager = (IndexManager) applicationContext.getBean("IndexManager", instanceId);
                indexManagers.putIfAbsent(instanceId, indexManager);
            } else {
                throw new JasDBStorageException("Unable to get index manager, instance does not exist");
            }
        }

        return indexManagers.get(instanceId);
    }

    @Override
    public void shutdownIndexes() throws JasDBStorageException {
        for(IndexManager indexManager : indexManagers.values()) {
            indexManager.shutdownIndexes();
        }
    }
}
