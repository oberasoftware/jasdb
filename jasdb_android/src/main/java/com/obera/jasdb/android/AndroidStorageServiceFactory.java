package com.obera.jasdb.android;

import com.google.inject.Inject;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.LocalStorageServiceFactoryImpl;
import nl.renarj.jasdb.service.LocalStorageServiceImpl;
import nl.renarj.jasdb.service.MachineGuidGenerator;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;

/**
 * @author renarj
 */
public class AndroidStorageServiceFactory extends LocalStorageServiceFactoryImpl {

    private DataOperationFactory dataOperationFactory;

    private IndexManagerFactory indexManagerFactory;

    private RecordWriterFactoryLoader recordWriterFactoryLoader;

    private MetadataStore metadataStore;

    private PartitioningManager partitioningManager;

    @Inject
    public AndroidStorageServiceFactory(DataOperationFactory dataOperationFactory,
                                        IndexManagerFactory indexManagerFactory,
                                        RecordWriterFactoryLoader recordWriterFactoryLoader,
                                        MetadataStore metadataStore,
                                        PartitioningManager partitioningManager) {
        this.dataOperationFactory = dataOperationFactory;
        this.indexManagerFactory = indexManagerFactory;
        this.recordWriterFactoryLoader = recordWriterFactoryLoader;
        this.metadataStore = metadataStore;
        this.partitioningManager = partitioningManager;
    }

    @Override
    protected StorageService createStorageServiceInstance(Instance instance, String bagName) throws JasDBStorageException {
        LocalStorageServiceImpl storageService = new LocalStorageServiceImpl(instance.getInstanceId(), bagName);
        storageService.setBagInsertOperation(dataOperationFactory.getInsertOperation());
        storageService.setBagRemoveOperation(dataOperationFactory.getRemoveOperation());
        storageService.setBagUpdateOperation(dataOperationFactory.getUpdateOperation());
        storageService.setBagPersistOperation(dataOperationFactory.getPersistOperation());
        storageService.setIndexManagerFactory(indexManagerFactory);
        storageService.setRecordWriterFactoryLoader(recordWriterFactoryLoader);
        storageService.setGenerator(new MachineGuidGenerator());
        storageService.setMetadataStore(metadataStore);
        storageService.setPartitionManager(partitioningManager);

        return storageService;
    }
}
