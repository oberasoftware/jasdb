package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * Main factory interface for creating storage services that allow operations on bag data
 * inside an instance.
 */
public interface StorageServiceFactory {
    StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException;

	StorageService getOrCreateStorageService(String instanceId, String bagName) throws JasDBStorageException;

    void removeStorageService(String instanceId, String bagName) throws JasDBStorageException;

    void removeAllStorageService(String instanceId) throws JasDBStorageException;
	
	void shutdownServiceFactory() throws JasDBStorageException;
}
