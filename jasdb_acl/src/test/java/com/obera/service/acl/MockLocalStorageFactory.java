package com.obera.service.acl;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author renarj
 */
@Component
public class MockLocalStorageFactory implements StorageServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MockLocalStorageFactory.class);

    @Autowired
    private StorageService storageService;

    @Override
    public StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException {
        return storageService;
    }

    @Override
    public StorageService getOrCreateStorageService(String instanceId, String bagName) throws JasDBStorageException {
        return storageService;
    }

    @Override
    public void removeStorageService(String instanceId, String bagName) throws JasDBStorageException {

    }

    @Override
    public void removeAllStorageService(String instanceId) throws JasDBStorageException {

    }

    @Override
    public void shutdownServiceFactory() throws JasDBStorageException {

    }
}
