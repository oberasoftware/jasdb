package com.oberasoftware.jasdb.engine.metadata;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class BagMetadataProvider implements MetadataProvider {

    private MetadataStore metadataStore;

    private InstanceMetadataProvider instanceMetadataProvider;

    private Map<String, MetaWrapper<Bag>> bagMetaMap = new ConcurrentHashMap<>();

    @Autowired
    public BagMetadataProvider(MetadataStore metadataStore, InstanceMetadataProvider instanceMetadataProvider) {
        this.metadataStore = metadataStore;
        this.instanceMetadataProvider = instanceMetadataProvider;
    }

    @PostConstruct
    public void loadData() throws JasDBStorageException {
        List<Entity> entities = metadataStore.getMetadataEntities(Constants.BAG_TYPE);

        for(Entity entity: entities) {
            UUID metaKey = UUID.fromString(entity.getInternalId());
            BagMeta bagMeta = BagMeta.fromEntity(entity);
            String bagId = getBagKey(bagMeta.getInstanceId(), bagMeta.getName());
            bagMetaMap.put(bagId, new MetaWrapper<>(bagMeta, metaKey));
        }
    }

    @Override
    public String getMetadataType() {
        return Constants.BAG_TYPE;
    }

    private String getBagKey(String instanceId, String bag) {
        return instanceId + "_" + bag;
    }

    public void addBag(Bag bag) throws JasDBStorageException {
        if(instanceMetadataProvider.containsInstance(bag.getInstanceId())) {
            String bagId = getBagKey(bag.getInstanceId(), bag.getName());
            if(!bagMetaMap.containsKey(bagId)) {
                SimpleEntity entity = BagMeta.toEntity(bag);

                UUID id = metadataStore.addMetadataEntity(entity);
                bagMetaMap.put(bagId, new MetaWrapper<>(bag, id));
            } else {
                throw new JasDBStorageException("Unable to add bag: " + bag.getName() + ", already exists");
            }
        } else {
            throw new JasDBStorageException("Unable to create bag, instance: " + bag.getInstanceId() + " does not exist");
        }
    }

    public void removeBag(String instanceId, String name) throws JasDBStorageException {
        String bagId = getBagKey(instanceId, name);
        if(bagMetaMap.containsKey(bagId)) {
            MetaWrapper<Bag> bagMetaWrapper = bagMetaMap.get(bagId);
            metadataStore.deleteMetadataEntity(bagMetaWrapper.getKey());

            bagMetaMap.remove(bagId);
        } else {
            throw new JasDBStorageException("Unable to delete bag: " + name + " does not exist");
        }
    }

    public void addBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
        Bag bag = getBag(instanceId, bagName);
        if(bag != null) {
            List<IndexDefinition> indexDefinitions = new ArrayList<>(bag.getIndexDefinitions());
            if(!indexDefinitions.contains(indexDefinition)) {
                indexDefinitions.add(indexDefinition);

                updateBag(instanceId, bagName, new BagMeta(instanceId, bagName, indexDefinitions));
            }
        } else {
            throw new JasDBStorageException("Unable to add index to bag: " + bagName + ",could not be found");
        }
    }

    public void removeBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
        Bag bag = getBag(instanceId, bagName);
        if(bag != null) {
            List<IndexDefinition> indexDefinitions = new ArrayList<>(bag.getIndexDefinitions());
            if(indexDefinitions.contains(indexDefinition)) {
                indexDefinitions.remove(indexDefinition);

                updateBag(instanceId, bagName, new BagMeta(instanceId, bagName, indexDefinitions));
            }
        } else {
            throw new JasDBStorageException("Unable to remove index for bag: " + bagName + ",could not be found");
        }
    }

    public List<Bag> getBags(String instanceId) {
        List<Bag> bags = new ArrayList<>();
        for(Map.Entry<String, MetaWrapper<Bag>> bagEntry : bagMetaMap.entrySet()) {
            if(bagEntry.getKey().startsWith(instanceId)) {
                bags.add(bagEntry.getValue().getMetadataObject());
            }
        }
        return bags;
    }

    public Bag getBag(String instanceId, String name) {
        return containsBag(instanceId, name) ? bagMetaMap.get(getBagKey(instanceId, name)).getMetadataObject() : null;
    }

    public boolean containsBag(String instanceId, String bag) {
        return bagMetaMap.containsKey(getBagKey(instanceId, bag));
    }

    /**
     * Small helper method to update the bag definition
     * @param instanceId The instanceId
     * @param bagName The name of the bag
     * @param newBagData The new updated bag data
     * @throws JasDBStorageException If unable to update the bag
     */
    private void updateBag(String instanceId, String bagName, Bag newBagData) throws JasDBStorageException {
        String bagId = getBagKey(instanceId, bagName);
        MetaWrapper<Bag> bagMetaWrapper = bagMetaMap.get(bagId);

        Entity updateData = BagMeta.toEntity(newBagData);
        updateData.setInternalId(bagMetaWrapper.getKey().toString());
        metadataStore.updateMetadataEntity(updateData);
        bagMetaWrapper.setMetadataObject(newBagData);
    }

}
