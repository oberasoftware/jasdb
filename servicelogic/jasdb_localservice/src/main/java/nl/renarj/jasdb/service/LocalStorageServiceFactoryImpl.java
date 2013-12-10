package nl.renarj.jasdb.service;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManagerFactory;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.metadata.BagMeta;
import nl.renarj.jasdb.service.wrappers.ServiceWrapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LocalStorageServiceFactoryImpl implements StorageServiceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageServiceFactoryImpl.class);

    private static final String DEFAULT_FLUSH_INTERVAL = "30s";

	private Map<String, StorageService> storageServices = new ConcurrentHashMap<>();

    private StorageFlushThread storageFlushThread;

    @Autowired
    private ConfigurationLoader configurationLoader;

    @Autowired
    private MetadataStore metadataStore;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ServiceWrapperFactory serviceWrapperFactory;

    @Autowired
    private IndexManagerFactory indexManagerFactory;

    @PostConstruct
    public void initializeServices() throws JasDBStorageException {
        loadFlushingMode();
    }

    private void loadFlushingMode() throws JasDBStorageException {
        Configuration flushingConfiguration = configurationLoader.getConfiguration().getChildConfiguration("/jasdb/flushing");
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
    public StorageService getStorageService(String instanceId, String bagName) throws JasDBStorageException {
        String storageServiceKey = instanceId + "_" + bagName;
        if(storageServices.containsKey(storageServiceKey)) {
            return storageServices.get(storageServiceKey);
        } else if(metadataStore.containsBag(instanceId, bagName)) {
            return getOrCreateStorageService(instanceId, bagName);
        }
        return null;
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
                serviceInstance.openService(configurationLoader.getConfiguration());
                serviceInstance.initializePartitions();
//                StorageService wrappedInstance = serviceWrapperFactory.wrap(serviceInstance);

                storageServices.put(key, serviceInstance); //wrappedInstance);
//                return wrappedInstance;
                return serviceInstance;
            } else {
                throw new JasDBStorageException("Unable to create bag storage service, instance: " + instanceId + " does not exist");
            }
        }
    }
	
	protected StorageService createStorageServiceInstance(Instance instance, String bagName) throws JasDBStorageException {
//        File bagFile = new File(instance.getPath(), bagName + LocalStorageServiceImpl.BAG_EXTENSION);
//        RecordWriter recordWriter = recordWriterFactory.createWriter(bagFile);

        return (StorageService) applicationContext.getBean("LocalStorageService", instance.getInstanceId(), bagName);
//		return new LocalStorageServiceImpl(instance.getInstanceId(), bagName); //indexManagerFactory.getIndexManager(instance), recordWriter, metadataStore, bagName);
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
        return new ArrayList<>(Collections2.transform(metadataStore.getBags(instanceId), new Function<Bag, String>() {
            @Override
            public String apply(Bag bag) {
                return bag.getName();
            }
        }));
	}
}
