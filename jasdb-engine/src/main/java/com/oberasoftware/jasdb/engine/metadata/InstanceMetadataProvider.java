package com.oberasoftware.jasdb.engine.metadata;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.oberasoftware.jasdb.engine.metadata.JasDBMetadataStore.DEFAULT_INSTANCE;

@Component
public class InstanceMetadataProvider implements MetadataProvider {

    private Map<String, MetaWrapper<Instance>> instanceMetaMap = new ConcurrentHashMap<>();

    private MetadataStore metadataStore;

    @Override
    public String getMetadataType() {
        return Constants.INSTANCE_TYPE;
    }

    @Autowired
    public InstanceMetadataProvider(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @PostConstruct
    public void loadData() throws JasDBStorageException {
        List<Entity> entities = metadataStore.getMetadataEntities(Constants.INSTANCE_TYPE);

        for(Entity entity: entities) {
            UUID metaKey = UUID.fromString(entity.getInternalId());

            InstanceMeta instance = InstanceMeta.fromEntity(entity);
            instanceMetaMap.put(instance.getInstanceId(), new MetaWrapper<>(instance, metaKey));
        }
        ensureDefaultInstance();
    }

    private void ensureDefaultInstance() throws JasDBStorageException {
        if(!instanceMetaMap.containsKey(DEFAULT_INSTANCE)) {
            addInstance(DEFAULT_INSTANCE, metadataStore.getDatastoreLocation().toString());
        }
    }

    public List<Instance> getInstances() {
        List<Instance> instances = new ArrayList<>();
        for(MetaWrapper<Instance> instanceMeta : instanceMetaMap.values()) {
            instances.add(instanceMeta.getMetadataObject());
        }
        return instances;
    }

    public Instance getInstance(String instanceId) {
        MetaWrapper<Instance> metaWrapper = instanceMetaMap.get(instanceId);
        if(metaWrapper != null) {
            return metaWrapper.getMetadataObject();
        } else {
            return null;
        }
    }

    public boolean containsInstance(String instanceId) {
        return instanceMetaMap.containsKey(instanceId);
    }

    public Instance addInstance(String instanceId) throws JasDBStorageException {
        return addInstance(instanceId, determinePath(instanceId));
    }

    private Instance addInstance(String instanceId, String path) throws JasDBStorageException {
        if(!instanceMetaMap.containsKey(instanceId)) {
            InstanceMeta instance = new InstanceMeta(instanceId, path);
            SimpleEntity entity = InstanceMeta.toEntity(instance);
            UUID key = metadataStore.addMetadataEntity(entity);

            instanceMetaMap.put(instanceId, new MetaWrapper<>(instance, key));
            return instance;
        } else {
            throw new JasDBStorageException("Unable to create instance, already exists");
        }
    }

    private String determinePath(String instanceId) throws JasDBStorageException {
        File instanceDirectory = new File(metadataStore.getDatastoreLocation(), instanceId);
        if(instanceDirectory.mkdirs()) {
            return instanceDirectory.toString();
        } else {
            throw new JasDBStorageException("Could not create instance storage directory: " + instanceDirectory.toString());
        }
    }

    public void removeInstance(String instanceId) throws JasDBStorageException {
        if(!DEFAULT_INSTANCE.equalsIgnoreCase(instanceId)) {
            MetaWrapper<Instance> instanceMetaWrapper = instanceMetaMap.get(instanceId);
            if(instanceMetaWrapper != null) {
                metadataStore.deleteMetadataEntity(instanceMetaWrapper.getKey());
                instanceMetaMap.remove(instanceId);
            } else {
                throw new JasDBStorageException("Unable to delete non existing instance: " + instanceId);
            }
        } else {
            throw new JasDBStorageException("Not allowed to remove default instance");
        }
    }

    public void updateInstance(Instance instance) throws JasDBStorageException {
        if(instanceMetaMap.containsKey(instance.getInstanceId())) {
            SimpleEntity entity = InstanceMeta.toEntity(instance);

            UUID key = metadataStore.updateMetadataEntity(entity);

            instanceMetaMap.put(instance.getInstanceId(), new MetaWrapper<>(instance, key));
        } else {
            throw new JasDBStorageException("Unable to update instance, does not exists");
        }
    }

}
