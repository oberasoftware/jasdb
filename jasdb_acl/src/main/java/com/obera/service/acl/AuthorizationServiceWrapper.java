package com.obera.service.acl;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.metadata.Constants;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import nl.renarj.jasdb.service.wrappers.ServiceWrapper;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class AuthorizationServiceWrapper implements ServiceWrapper {
    private StorageService wrappedService;
    private UserManager userManager;

    @Override
    public void wrap(KernelContext kernelContext, StorageService storageService) throws JasDBStorageException {
        this.wrappedService = storageService;
        this.userManager =  kernelContext.getInjector().getInstance(UserManager.class);
    }

    @Override
    public StorageService unwrap() throws JasDBStorageException {
        return wrappedService;
    }

    @Override
    public String getBagName() {
        return wrappedService.getBagName();
    }

    @Override
    public String getInstanceId() {
        return wrappedService.getInstanceId();
    }

    @Override
    public void openService(Configuration configuration) throws JasDBStorageException {
        wrappedService.openService(configuration);
    }

    @Override
    public void closeService() throws JasDBStorageException {
        wrappedService.closeService();
    }

    @Override
    public void flush() throws JasDBStorageException {
        wrappedService.flush();
    }

    @Override
    public void remove() throws JasDBStorageException {
        wrappedService.remove();
    }

    @Override
    public void initializePartitions() throws JasDBStorageException {
        wrappedService.initializePartitions();
    }

    @Override
    public PartitioningManager getPartitionManager() {
        return wrappedService.getPartitionManager();
    }

    @Override
    public void insertEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.WRITE);
        wrappedService.insertEntity(context, entity);
    }

    private String getObjectName() {
        return Constants.OBJECT_SEPARATOR + getInstanceId() + "/bags/" + getBagName();
    }

    @Override
    public void removeEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.DELETE);
        wrappedService.removeEntity(context, entity);
    }

    @Override
    public void removeEntity(RequestContext context, String internalId) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.DELETE);
        wrappedService.removeEntity(context, internalId);
    }

    @Override
    public void updateEntity(RequestContext context, SimpleEntity entity) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.UPDATE);
        wrappedService.updateEntity(context, entity);
    }

    @Override
    public long getSize() throws JasDBStorageException {
        return wrappedService.getSize();
    }

    @Override
    public long getDiskSize() throws JasDBStorageException {
        return wrappedService.getDiskSize();
    }

    @Override
    public SimpleEntity getEntityById(RequestContext requestContext, String id) throws JasDBStorageException {
        userManager.authorize(requestContext.getUserSession(), getObjectName(), AccessMode.READ);
        return wrappedService.getEntityById(requestContext, id);
    }

    @Override
    public QueryResult getEntities(RequestContext context) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.READ);
        return wrappedService.getEntities(context);
    }

    @Override
    public QueryResult getEntities(RequestContext context, int max) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.READ);
        return wrappedService.getEntities(context, max);
    }

    @Override
    public QueryResult search(RequestContext context, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException {
        userManager.authorize(context.getUserSession(), getObjectName(), AccessMode.READ);
        return wrappedService.search(context, blockOperation, limit, params);
    }

    @Override
    public void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        wrappedService.ensureIndex(indexField, isUnique, valueFields);
    }

    @Override
    public void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException {
        wrappedService.ensureIndex(indexField, isUnique, valueFields);
    }

    @Override
    public List<String> getIndexNames() throws JasDBStorageException {
        return wrappedService.getIndexNames();
    }

    @Override
    public void removeIndex(String indexName) throws JasDBStorageException {
        wrappedService.removeIndex(indexName);
    }
}
