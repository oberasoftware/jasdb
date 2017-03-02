package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataProvider;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.core.platform.PlatformManagerFactory;
import nl.renarj.jasdb.core.utils.FileUtils;
import nl.renarj.jasdb.storage.transactional.FSWriter;
import nl.renarj.jasdb.storage.transactional.RecordIteratorImpl;
import nl.renarj.jasdb.storage.transactional.RecordResultImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Optional.of;

/**
 * @author Renze de Vries
 */
@Component
@Singleton
public class JasDBMetadataStore implements MetadataStore {
    private static final Logger LOG = LoggerFactory.getLogger(JasDBMetadataStore.class);

    private static final String METADATA_FILE = "metadata.pjs";
    private static final String PID_FILE = "metadata.pid";

    public static final String DEFAULT_INSTANCE = "default";

    private FSWriter writer;
    private File datastoreLocation;

    private boolean lastShutdownClean = true;

    private Map<String, MetaWrapper<Instance>> instanceMetaMap = new ConcurrentHashMap<>();
    private Map<String, MetaWrapper<Bag>> bagMetaMap = new ConcurrentHashMap<>();

    private Map<String, MetadataProvider> metadataProviders = new ConcurrentHashMap<>();

    public JasDBMetadataStore() throws JasDBStorageException {
        openStore();
    }

    private void openStore() throws JasDBStorageException {
        datastoreLocation = HomeLocatorUtil.determineDatastoreLocation();

        handleCreateNewPidFile();

        writer = new FSWriter(new File(datastoreLocation, METADATA_FILE));
        writer.openWriter();

        ServiceLoader<MetadataProvider> providers = ServiceLoader.load(MetadataProvider.class);
        for(MetadataProvider provider : providers) {
            metadataProviders.put(provider.getMetadataType(), provider);
            provider.setMetadataStore(this);
        }

        loadRecords();
        ensureDefaultInstance();
    }

    private void handleCreateNewPidFile() throws JasDBStorageException {
        File pidFile = new File(datastoreLocation, PID_FILE);
        lastShutdownClean = !pidFile.exists();
        LOG.info("Last shutdown clean: {}", lastShutdownClean);
        try {
            if(datastoreLocation.exists() || datastoreLocation.mkdirs()) {
                FileUtils.writeToFile(pidFile, "JasDB Instance Started, " + PlatformManagerFactory.getPlatformManager().getProcessId());
                LOG.info("Created JasDB pid file: {}", pidFile);
            } else {
                throw new JasDBStorageException("Unable to create database path, directories could not be created: " + datastoreLocation) ;
            }
        } catch(IOException e) {
            throw new JasDBStorageException("Unable to open JasDB metadata store, pid file: " + pidFile.toString() + " could not be created");
        }
    }

    private void loadRecords() throws JasDBStorageException {
        for(RecordIteratorImpl recordIterator = writer.readAllRecords(); recordIterator.hasNext(); ) {
            RecordResultImpl recordResult = recordIterator.next();
            SimpleEntity entity = SimpleEntity.fromStream(recordResult.getStream());
            String type = entity.getValue(Constants.META_TYPE).toString();
            if(type.equals(Constants.INSTANCE_TYPE)) {
                InstanceMeta instance = InstanceMeta.fromEntity(entity);
                instanceMetaMap.put(instance.getInstanceId(), new MetaWrapper<Instance>(instance, recordResult.getRecordPointer()));
            } else if(type.equals(Constants.BAG_TYPE)) {
                BagMeta bagMeta = BagMeta.fromEntity(entity);
                String bagId = getBagKey(bagMeta.getInstanceId(), bagMeta.getName());
                bagMetaMap.put(bagId, new MetaWrapper<Bag>(bagMeta, recordResult.getRecordPointer()));
            } else if(metadataProviders.containsKey(type)) {
                metadataProviders.get(type).registerMetadataEntity(entity, recordResult.getRecordPointer());
            } else {
                throw new JasDBStorageException("Unable to load metadata record: " + entity.toString() + " unknown type: " + type);
            }
        }
    }

    @Override
    public boolean isLastShutdownClean() throws JasDBStorageException {
        return lastShutdownClean;
    }

    private void ensureDefaultInstance() throws JasDBStorageException {
        if(!instanceMetaMap.containsKey(DEFAULT_INSTANCE)) {
            addInstance(new InstanceMeta(DEFAULT_INSTANCE, datastoreLocation.toString()));
        }
    }

    @Override
    @PreDestroy
    public void closeStore() throws JasDBStorageException {
        if(writer != null) {
            writer.closeWriter();
            writer = null;

            File pidFile = new File(datastoreLocation, PID_FILE);
            if(!pidFile.delete()) {
                LOG.warn("PID file: {} could not be removed", pidFile);
            }
        }
    }

    @Override
    public long addMetadataEntity(SimpleEntity entity) throws JasDBStorageException {
        return writer.writeRecord(SimpleEntity.toJson(entity), null);
    }

    @Override
    public long updateMetadataEntity(SimpleEntity entity, long previousRecord) throws JasDBStorageException {
        return writer.updateRecord(SimpleEntity.toJson(entity), () -> of(previousRecord), null);
    }

    @Override
    public void deleteMetadataEntity(long recordPointer) throws JasDBStorageException {
        writer.removeRecord(() -> of(recordPointer), null);
    }

    @Override
    public List<Bag> getBags(String instanceId) throws JasDBStorageException {
        List<Bag> bags = new ArrayList<>();
        for(Map.Entry<String, MetaWrapper<Bag>> bagEntry : bagMetaMap.entrySet()) {
            if(bagEntry.getKey().startsWith(instanceId)) {
                bags.add(bagEntry.getValue().getMetadataObject());
            }
        }
        return bags;
    }

    @Override
    public Bag getBag(String instanceId, String name) throws JasDBStorageException {
        MetaWrapper<Bag> bagWrapper = bagMetaMap.get(getBagKey(instanceId, name));
        if(bagWrapper != null) {
            return bagWrapper.getMetadataObject();
        } else {
            return null;
        }
    }

    @Override
    public boolean containsBag(String instanceId, String bag) throws JasDBStorageException {
        return bagMetaMap.containsKey(getBagKey(instanceId, bag));
    }

    private String getBagKey(String instanceId, String bag) {
        return instanceId + "_" + bag;
    }

    @Override
    public void addBag(Bag bag) throws JasDBStorageException {
        if(instanceMetaMap.containsKey(bag.getInstanceId())) {
            String bagId = getBagKey(bag.getInstanceId(), bag.getName());
            if(!bagMetaMap.containsKey(bagId)) {
                SimpleEntity entity = BagMeta.toEntity(bag);
                String bagData = SimpleEntity.toJson(entity);
                long recordPointer = writer.writeRecord(bagData, null);
                bagMetaMap.put(bagId, new MetaWrapper<>(bag, recordPointer));
            } else {
                throw new JasDBStorageException("Unable to add bag: " + bag.getName() + ", already exists");
            }
        } else {
            throw new JasDBStorageException("Unable to create bag, instance: " + bag.getInstanceId() + " does not exist");
        }
    }

    @Override
    public void removeBag(String instanceId, String name) throws JasDBStorageException {
        String bagId = getBagKey(instanceId, name);
        if(bagMetaMap.containsKey(bagId)) {
            MetaWrapper<Bag> bagMetaWrapper = bagMetaMap.get(bagId);
            writer.removeRecord(() -> of(bagMetaWrapper.getRecordPointer()), null);
            bagMetaMap.remove(bagId);
        } else {
            throw new JasDBStorageException("Unable to delete bag: " + name + " does not exist");
        }
    }

    @Override
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

    @Override
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

        long recordPointer = writer.updateRecord(SimpleEntity.toJson(BagMeta.toEntity(newBagData)), () -> of(bagMetaWrapper.getRecordPointer()), null);
        bagMetaWrapper.setRecordPointer(recordPointer);
        bagMetaWrapper.setMetadataObject(newBagData);
    }

    @Override
    public boolean containsIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
        Bag bag = getBag(instanceId, bagName);
        return bag.getIndexDefinitions().contains(indexDefinition);
    }

    @Override
    public List<Instance> getInstances() throws JasDBStorageException {
        List<Instance> instances = new ArrayList<>();
        for(MetaWrapper<Instance> instanceMeta : instanceMetaMap.values()) {
            instances.add(instanceMeta.getMetadataObject());
        }
        return instances;
    }

    @Override
    public Instance getInstance(String instanceId) throws JasDBStorageException {
        MetaWrapper<Instance> metaWrapper = instanceMetaMap.get(instanceId);
        if(metaWrapper != null) {
            return metaWrapper.getMetadataObject();
        } else {
            return null;
        }
    }

    @Override
    public boolean containsInstance(String instanceId) throws JasDBStorageException {
        return instanceMetaMap.containsKey(instanceId);
    }

    @Override
    public void addInstance(Instance instance) throws JasDBStorageException {
        if(!instanceMetaMap.containsKey(instance.getInstanceId())) {
            SimpleEntity entity = InstanceMeta.toEntity(instance);
            String jsonData = SimpleEntity.toJson(entity);
            long recordPointer = writer.writeRecord(jsonData, null);

            instanceMetaMap.put(instance.getInstanceId(), new MetaWrapper<>(instance, recordPointer));
        } else {
            throw new JasDBStorageException("Unable to create instance, already exists");
        }
    }

    @Override
    public void removeInstance(String instanceId) throws JasDBStorageException {
        if(!DEFAULT_INSTANCE.equalsIgnoreCase(instanceId)) {
            MetaWrapper<Instance> instanceMetaWrapper = instanceMetaMap.get(instanceId);
            if(instanceMetaWrapper != null) {
                writer.removeRecord(() -> of(instanceMetaWrapper.getRecordPointer()), null);
                instanceMetaMap.remove(instanceId);
            } else {
                throw new JasDBStorageException("Unable to delete non existing instance: " + instanceId);
            }
        } else {
            throw new JasDBStorageException("Not allowed to remove default instance");
        }
    }

    @Override
    public void updateInstance(Instance instance) throws JasDBStorageException {
        if(instanceMetaMap.containsKey(instance.getInstanceId())) {
            SimpleEntity entity = InstanceMeta.toEntity(instance);
            String jsonData = SimpleEntity.toJson(entity);

            removeInstance(instance.getInstanceId());
            long recordPointer = writer.writeRecord(jsonData, null);

            instanceMetaMap.put(instance.getInstanceId(), new MetaWrapper<>(instance, recordPointer));
        } else {
            throw new JasDBStorageException("Unable to update instance, does not exists");
        }
    }

    @Override
    public <T extends MetadataProvider> T getMetadataProvider(String type) {
        return (T)metadataProviders.get(type);
    }

}
