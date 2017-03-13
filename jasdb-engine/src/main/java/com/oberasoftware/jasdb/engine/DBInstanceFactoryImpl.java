package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.engine.metadata.JasDBMetadataStore;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DBInstanceFactoryImpl implements DBInstanceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DBInstanceFactoryImpl.class);

	private Map<String, DBInstance> instances = new ConcurrentHashMap<>();

    private final MetadataStore metadataStore;

    private final StorageServiceFactory storageServiceFactory;

	@Autowired
	public DBInstanceFactoryImpl(MetadataStore metadataStore, StorageServiceFactory storageServiceFactory) throws JasDBStorageException {
        this.metadataStore = metadataStore;
        this.storageServiceFactory = storageServiceFactory;

        initializeServices();
    }

    private void initializeServices() throws JasDBStorageException {
        for(Instance instanceMeta : metadataStore.getInstances()) {
            LOG.info("Loading instance: {} on path: {}", instanceMeta.getInstanceId(), instanceMeta.getPath());
            instances.put(instanceMeta.getInstanceId(), new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta));
        }
    }

    @Override
    public void addInstance(String instanceId) throws JasDBStorageException {
        if(!metadataStore.containsInstance(instanceId)) {
            Instance instanceData = metadataStore.addInstance(instanceId);
            this.instances.put(instanceId, new DBInstanceImpl(storageServiceFactory, metadataStore, instanceData));
        } else {
            throw new JasDBStorageException("Instance with id: " + instanceId + " was already configured, can't override");
        }
    }

    @Override
    public void deleteInstance(String instanceId) throws JasDBStorageException {
        if(instances.containsKey(instanceId)) {
            try {
                storageServiceFactory.removeAllStorageService(instanceId);

                metadataStore.removeInstance(instanceId);
                instances.remove(instanceId);
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
