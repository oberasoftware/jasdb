package nl.renarj.jasdb.service;

import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * Main factory interface for creating storage services that allow operations on bag data
 * inside an instance.
 */
public interface StorageServiceFactory {
    void initializeServices(KernelContext kernelContext) throws JasDBStorageException;
    
    IndexManager getIndexManager(Instance instance) throws JasDBStorageException;

    StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException;

	StorageService getOrCreateStorageService(String instanceId, String bagName) throws JasDBStorageException;

    void removeStorageService(String instanceId, String bagName) throws JasDBStorageException;

    void removeAllStorageService(String instanceId) throws JasDBStorageException;
	
	void shutdownServiceFactory() throws JasDBStorageException;
	
    void initializeInstanceBags(String instance) throws JasDBStorageException;
}
