package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.engine.metadata.BagMeta;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.core.utils.conversion.ValueConverterUtil;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.engine.ConfigurationLoader;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
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
import java.util.stream.Collectors;

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

                storageServices.put(key, serviceInstance); //wrappedInstance);
                return serviceInstance;
            } else {
                throw new JasDBStorageException("Unable to create bag storage service, instance: " + instanceId + " does not exist");
            }
        }
    }
	
	protected StorageService createStorageServiceInstance(Instance instance, String bagName) throws JasDBStorageException {
        return (StorageService) applicationContext.getBean("LocalStorageService", instance.getInstanceId(), bagName);
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
        return metadataStore.getBags(instanceId).stream()
                .map(Bag::getName)
                .collect(Collectors.toList());
	}
}
