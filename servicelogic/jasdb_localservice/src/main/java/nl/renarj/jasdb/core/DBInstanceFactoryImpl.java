package nl.renarj.jasdb.core;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.service.metadata.InstanceMeta;
import nl.renarj.jasdb.service.metadata.JasDBMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DBInstanceFactoryImpl implements DBInstanceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DBInstanceFactoryImpl.class);

	private Map<String, DBInstance> instances = new ConcurrentHashMap<>();

//    @Inject
    private MetadataStore metadataStore;

    @Inject
	public DBInstanceFactoryImpl(MetadataStore metadataStore) throws JasDBStorageException {
        this.metadataStore = metadataStore;
        initializeServices();
	}

//    @PostConstruct
    public void initializeServices() throws JasDBStorageException {
        for(Instance instanceMeta : metadataStore.getInstances()) {
            LOG.info("Loading instance: {} on path: {}", instanceMeta.getInstanceId(), instanceMeta.getPath());
            instances.put(instanceMeta.getInstanceId(), new DBInstanceImpl(metadataStore, instanceMeta));
        }
    }

	private void addInstance(String instanceId, DBInstance instance) throws JasDBStorageException {
        if(!metadataStore.containsInstance(instanceId)) {
            metadataStore.addInstance(new InstanceMeta(instanceId, instance.getPath()));
		    this.instances.put(instanceId, instance);
        } else {
            throw new JasDBStorageException("Instance with id: " + instanceId + " was already configured, can't override");
        }
	}

    @Override
    public void addInstance(String instanceId, String path) throws JasDBStorageException {
        addInstance(instanceId, new DBInstanceImpl(metadataStore, new InstanceMeta(instanceId, path)));
    }

    @Override
    public void deleteInstance(String instanceId) throws JasDBStorageException {
        if(instances.containsKey(instanceId)) {
            try {
                StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
                storageServiceFactory.removeAllStorageService(instanceId);

                metadataStore.removeInstance(instanceId);
            } catch(ConfigurationException e) {
                throw new JasDBStorageException("Unable to remove bags from instance: " + instanceId, e);
            }
        } else {
            throw new JasDBStorageException("Unable to delete instance: " + instanceId + " does not exist");
        }
    }

    @Override
	public DBInstance getInstance() throws ConfigurationException {
        return getInstance(JasDBMetadataStore.DEFAULT_INSTANCE);
	}

    @Override
	public DBInstance getInstance(String instanceId) throws ConfigurationException {
		if(StringUtils.stringEmpty(instanceId)) {
			return getInstance();
		} else {
			if(instances.containsKey(instanceId)) {
				return instances.get(instanceId);
			} else {
				throw new ConfigurationException("No instance was found for id: " + instanceId);
			}
		}
	}

    @Override
    public boolean hasInstance(String instanceId) {
        return StringUtils.stringNotEmpty(instanceId) && instances.containsKey(instanceId);
    }

    @Override
	public List<DBInstance> listInstances() {
		return new ArrayList<>(this.instances.values());
	}

	@Override
	public void shutdown() throws ConfigurationException {
	}
}
