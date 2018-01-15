package com.oberasoftware.jasdb.cluster.storage;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.cluster.api.ClusterManager;
import com.oberasoftware.jasdb.engine.LocalStorageServiceFactoryImpl;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class ClusterAwareStorageServiceFactory implements StorageServiceFactory{

    private final LocalStorageServiceFactoryImpl localStorageServiceFactory;
    private final ClusterManager clusterManager;

    @Autowired
    public ClusterAwareStorageServiceFactory(LocalStorageServiceFactoryImpl localStorageServiceFactory, ClusterManager clusterManager) {
        this.localStorageServiceFactory = localStorageServiceFactory;
        this.clusterManager = clusterManager;
    }

    @Override
    public StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException {
        return localStorageServiceFactory.getStorageService(instanceId, bagName);
    }

    @Override
    public StorageService getOrCreateStorageService(String instanceId, String bagName) throws JasDBStorageException {
        return localStorageServiceFactory.getOrCreateStorageService(instanceId, bagName);
    }

    @Override
    public void removeStorageService(String instanceId, String bagName) throws JasDBStorageException {
        localStorageServiceFactory.removeStorageService(instanceId, bagName);
    }

    @Override
    public void removeAllStorageService(String instanceId) throws JasDBStorageException {
        localStorageServiceFactory.removeAllStorageService(instanceId);
    }

    @Override
    public void shutdownServiceFactory() throws JasDBStorageException {
        localStorageServiceFactory.shutdownServiceFactory();
    }
}
