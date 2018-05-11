package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Component
public class MockLocalStorageFactory implements StorageServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MockLocalStorageFactory.class);

    @Autowired
    @Qualifier("mockStorageService")
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
