package com.obera.jasdb.android;

import com.google.inject.Inject;
import com.google.inject.Provider;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.service.operations.BagInsertOperation;
import nl.renarj.jasdb.service.operations.BagPersistOperation;
import nl.renarj.jasdb.service.operations.BagRemoveOperation;
import nl.renarj.jasdb.service.operations.BagUpdateOperation;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;

/**
 * @author renarj
 */
public class DataOperationFactory {

    private Provider<IndexManagerFactory> indexManagerFactoryProvider;
    private Provider<RecordWriterFactoryLoader> recordWriterFactoryLoaderProvider;

    @Inject
    public DataOperationFactory(Provider<IndexManagerFactory> indexManagerFactoryProvider,
                                Provider<RecordWriterFactoryLoader> recordWriterFactoryLoaderProvider) {
        this.indexManagerFactoryProvider = indexManagerFactoryProvider;
        this.recordWriterFactoryLoaderProvider = recordWriterFactoryLoaderProvider;
    }

    public BagInsertOperation getInsertOperation() {
        BagInsertOperation insertOperation = new BagInsertOperation();
        insertOperation.setIndexManagerFactory(indexManagerFactoryProvider.get());
        insertOperation.setRecordWriterFactory(recordWriterFactoryLoaderProvider.get());
        return insertOperation;
    }

    public BagRemoveOperation getRemoveOperation() {
        BagRemoveOperation removeOperation = new BagRemoveOperation();
        removeOperation.setIndexManagerFactory(indexManagerFactoryProvider.get());
        removeOperation.setRecordWriterFactoryLoader(recordWriterFactoryLoaderProvider.get());
        return removeOperation;
    }

    public BagUpdateOperation getUpdateOperation() {
        BagUpdateOperation updateOperation = new BagUpdateOperation();
        updateOperation.setRecordWriterFactoryLoader(recordWriterFactoryLoaderProvider.get());
        updateOperation.setIndexManagerFactory(indexManagerFactoryProvider.get());
        return updateOperation;
    }

    public BagPersistOperation getPersistOperation() {
        BagPersistOperation persistOperation = new BagPersistOperation();
        persistOperation.setInsertOperation(getInsertOperation());
        persistOperation.setRecordWriterFactory(recordWriterFactoryLoaderProvider.get());
        persistOperation.setUpdateOperation(getUpdateOperation());
        return persistOperation;
    }
}
