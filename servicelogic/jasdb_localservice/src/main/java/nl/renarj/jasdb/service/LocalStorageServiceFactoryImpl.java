package nl.renarj.jasdb.service;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;
import nl.renarj.jasdb.service.metadata.BagMeta;
import nl.renarj.jasdb.service.wrappers.ServiceWrapperFactory;
import nl.renarj.jasdb.storage.indexing.IndexManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class LocalStorageServiceFactoryImpl implements StorageServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageServiceFactoryImpl.class);

    private static final String DEFAULT_FLUSH_INTERVAL = "30s";

	private Map<String, StorageService> storageServices = new ConcurrentHashMap<String, StorageService>();
	
    private Map<String, IndexManager> indexManagers = new ConcurrentHashMap<String, IndexManager>();
    
    private MetadataStore metadataStore;
    private RecordWriterFactory recordWriterFactory;
    private ServiceWrapperFactory serviceWrapperFactory;
    protected Configuration configuration;

    private StorageFlushThread storageFlushThread;
	
	@Inject
	public LocalStorageServiceFactoryImpl(Configuration configuration, RecordWriterFactory recordWriterFactory) throws ConfigurationException {
        this.configuration = configuration;
        this.recordWriterFactory = recordWriterFactory;
	}

    @Override
    public void initializeServices(KernelContext kernelContext) throws JasDBStorageException {
        this.metadataStore = kernelContext.getMetadataStore();
        this.serviceWrapperFactory = new ServiceWrapperFactory(kernelContext);

        for(Instance instance : metadataStore.getInstances()) {
            initializeInstanceBags(instance.getInstanceId());
        }

        loadFlushingMode();
    }

    private void loadFlushingMode() throws JasDBStorageException {
        Configuration flushingConfiguration = configuration.getChildConfiguration("/jasdb/flushing");
        if(flushingConfiguration != null && flushingConfiguration.getAttribute("enabled", false) && flushingConfiguration.getAttribute("mode", "").equals("interval")) {
            Configuration periodConfiguration = flushingConfiguration.getChildConfiguration("Property[@Name='period']");
            int period = 30000;
            try {
                period = (int) ValueConverterUtil.convertToMilliseconds(periodConfiguration.getAttribute("Value", DEFAULT_FLUSH_INTERVAL));
            } catch(CoreConfigException e) {
                LOG.warn("Could not convert flushing period in configuration", e);
            }

            LOG.info("Flushing mode set to interval period: {}", period);
            storageFlushThread = new StorageFlushThread(storageServices, period);
            storageFlushThread.start();
        }
    }
    
    @Override
    public void initializeInstanceBags(String instance) throws JasDBStorageException {
        for(String bag : getBags(instance)) {
            LOG.debug("Loading storage service for bag: {} for instance: {}", bag, instance);
            getOrCreateStorageService(instance, bag);
        }
    }

    @Override
    public IndexManager getIndexManager(Instance instance) throws JasDBStorageException {
        if(!indexManagers.containsKey(instance.getInstanceId())) {
            if(metadataStore.containsInstance(instance.getInstanceId())) {
                IndexManager indexManager = new IndexManagerImpl(metadataStore, instance, configuration);
                indexManagers.put(instance.getInstanceId(), indexManager);
            } else {
                throw new JasDBStorageException("Unable to get index manager, instance does not exist");
            }
        }

        return indexManagers.get(instance.getInstanceId());
    }

    @Override
    public StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException {
        String storageServiceKey = instanceId + "_" + bagName;
        if(storageServices.containsKey(storageServiceKey)) {
            return storageServices.get(storageServiceKey);
        } else {
            return null;
        }
    }

    @Override
	public StorageService getOrCreateStorageService(String instanceId, String bagName) throws JasDBStorageException {
        String storageServiceKey = instanceId + "_" + bagName;
		if(storageServices.containsKey(storageServiceKey)) {
			return storageServices.get(storageServiceKey);
		} else {
            return createStorageService(instanceId, bagName, storageServiceKey);
		}
	}

    private synchronized StorageService createStorageService(String instanceId, String bagName, String key) throws JasDBStorageException {
        //now we have a lock let's check again
        if(storageServices.containsKey(key)) {
            return storageServices.get(key);
        } else {
            if(metadataStore.containsInstance(instanceId)) {
                if(!metadataStore.containsBag(instanceId, bagName)) {
                    metadataStore.addBag(new BagMeta(instanceId, bagName, new ArrayList<IndexDefinition>()));
                }

                Instance instance = metadataStore.getInstance(instanceId);
                StorageService serviceInstance = createStorageServiceInstance(instance, bagName);
                serviceInstance.openService(configuration);
                serviceInstance.initializePartitions();
                StorageService wrappedInstance = serviceWrapperFactory.wrap(serviceInstance);

                storageServices.put(key, wrappedInstance);
                return wrappedInstance;
            } else {
                throw new JasDBStorageException("Unable to create bag storage service, instance: " + instanceId + " does not exist");
            }
        }
    }
	
	protected StorageService createStorageServiceInstance(Instance instance, String bagName) throws JasDBStorageException {
        File bagFile = new File(instance.getPath(), bagName + LocalStorageServiceImpl.BAG_EXTENSION);
        RecordWriter recordWriter = recordWriterFactory.createWriter(bagFile);

		return new LocalStorageServiceImpl(instance, getIndexManager(instance), recordWriter, metadataStore, bagName);
	}

    @Override
    public void removeStorageService(String instanceId, String bagName) throws JasDBStorageException {
        String storageServiceKey = instanceId + "_" + bagName;
        StorageService storageService = storageServices.get(storageServiceKey);
        if(storageService != null) {
            storageService.remove();
            storageServices.remove(storageServiceKey);

            metadataStore.removeBag(instanceId, bagName);
        } else {
            throw new JasDBStorageException("Unable to remove bag, bag does not exist");
        }
    }

    @Override
    public void removeAllStorageService(String instanceId) throws JasDBStorageException {
        for(String bag : getBags(instanceId)) {
            removeStorageService(instanceId, bag);
        }
    }

    @Override
	public void shutdownServiceFactory() throws JasDBStorageException {
        if(storageFlushThread != null) {
            storageFlushThread.stop();
        }

		for(StorageService service : storageServices.values()) {
			service.closeService();
		}
		storageServices.clear();
	}

	private List<String> getBags(String instanceId) throws JasDBStorageException {
        return new ArrayList<String>(Collections2.transform(metadataStore.getBags(instanceId), new Function<Bag, String>() {
            @Override
            public String apply(Bag bag) {
                return bag.getName();
            }
        }));
	}
}
