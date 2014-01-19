package com.obera.jasdb.android;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.DBInstanceFactoryImpl;
import nl.renarj.jasdb.service.JasDBConfigurationLoader;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.service.metadata.JasDBMetadataStore;
import nl.renarj.jasdb.service.partitioning.LocalPartitionManager;
import nl.renarj.jasdb.service.partitioning.PartitioningManager;
import nl.renarj.jasdb.storage.RecordWriterFactoryLoader;
import nl.renarj.jasdb.storage.indexing.IndexManagerImpl;

/**
 * @author renarj
 */
public class AndroidKernelBinding extends AbstractModule {

    @Override
    protected void configure() {
        bind(MetadataStore.class).to(JasDBMetadataStore.class);
        bind(DBInstanceFactory.class).to(DBInstanceFactoryImpl.class);
        bind(PartitioningManager.class).to(LocalPartitionManager.class);
        bind(StorageServiceFactory.class).to(AndroidStorageServiceFactory.class);
        bind(DataOperationFactory.class); //.to(DataOperationFactory.class);
        bind(IndexManagerFactory.class).to(AndroidIndexManagerFactory.class);
        bind(RecordWriterFactoryLoader.class); //.to(RecordWriterFactoryLoader.class);
        bind(ConfigurationLoader.class).to(JasDBConfigurationLoader.class);
        install(new FactoryModuleBuilder().implement(IndexManager.class, IndexManagerImpl.class).build(GuiceIndexManagerFactory.class));
    }
}
