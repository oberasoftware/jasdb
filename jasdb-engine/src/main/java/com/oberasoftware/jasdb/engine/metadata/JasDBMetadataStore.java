package com.oberasoftware.jasdb.engine.metadata;

import com.oberasoftware.jasdb.api.engine.MetadataProviderFactory;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.utils.FileUtils;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.writer.transactional.RecordResultImpl;
import com.oberasoftware.jasdb.writer.transactional.TransactionalRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.oberasoftware.jasdb.core.SimpleEntity.toJson;
import static com.oberasoftware.jasdb.core.utils.RecordStreamUtil.toStream;

/**
 * @author Renze de Vries
 */
@Component
public class JasDBMetadataStore implements MetadataStore {
    private static final Logger LOG = LoggerFactory.getLogger(JasDBMetadataStore.class);

    private static final String METADATA_FILE = "metadata.pjs";
    private static final String PID_FILE = "metadata.pid";

    public static final String DEFAULT_INSTANCE = "default";

    private RecordWriter<UUIDKey> recordWriter;
    private File datastoreLocation;

    private boolean lastShutdownClean = true;

    public static String PID = ManagementFactory.getRuntimeMXBean().getName();

    private MetadataProviderFactory metadataProviderFactory;

    @Autowired
    public JasDBMetadataStore(MetadataProviderFactory metadataProviderFactory) throws JasDBStorageException {
        this.metadataProviderFactory = metadataProviderFactory;
        openStore();
    }

    @Override
    public File getDatastoreLocation() {
        return datastoreLocation;
    }

    private void openStore() throws JasDBStorageException {
        datastoreLocation = HomeLocatorUtil.determineDatastoreLocation();

        handleCreateNewPidFile();

        recordWriter = new TransactionalRecordWriter(new File(datastoreLocation, METADATA_FILE));
        recordWriter.openWriter();

    }

    private void handleCreateNewPidFile() throws JasDBStorageException {
        File pidFile = new File(datastoreLocation, PID_FILE);
        lastShutdownClean = !pidFile.exists();
        LOG.info("Last shutdown clean: {}", lastShutdownClean);
        try {
            if(datastoreLocation.exists() || datastoreLocation.mkdirs()) {
                FileUtils.writeToFile(pidFile, "JasDB Instance Started, " + PID);
                LOG.info("Created JasDB pid file: {}", pidFile);
            } else {
                throw new JasDBStorageException("Unable to create database path, directories could not be created: " + datastoreLocation) ;
            }
        } catch(IOException e) {
            throw new JasDBStorageException("Unable to open JasDB metadata store, pid file: " + pidFile.toString() + " could not be created");
        }
    }

    private InstanceMetadataProvider getInstanceProvider() {
        return metadataProviderFactory.getProvider(Constants.INSTANCE_TYPE);
    }

    private BagMetadataProvider getBagProvider() {
        return metadataProviderFactory.getProvider(Constants.BAG_TYPE);
    }

    @Override
    public boolean isLastShutdownClean() {
        return lastShutdownClean;
    }

    @Override
    @PreDestroy
    public void closeStore() throws JasDBStorageException {
        if(recordWriter != null) {
            recordWriter.closeWriter();
            recordWriter = null;

            File pidFile = new File(datastoreLocation, PID_FILE);
            if(!pidFile.delete()) {
                LOG.warn("PID file: {} could not be removed", pidFile);
            }
        }
    }

    @Override
    public UUID addMetadataEntity(Entity entity) throws JasDBStorageException {
        UUIDKey key = new UUIDKey(UUID.randomUUID());
        recordWriter.writeRecord(key, toStream(toJson(entity)));
        entity.setInternalId(key.getValue());

        return new UUID(key.getMostSignificant(), key.getLeastSignificant());
    }

    @Override
    public UUID updateMetadataEntity(Entity entity) throws JasDBStorageException {
        recordWriter.updateRecord(new UUIDKey(entity.getInternalId()), toStream(toJson(entity)));

        return UUID.fromString(entity.getInternalId());
    }

    @Override
    public void deleteMetadataEntity(UUID recordKey) throws JasDBStorageException {
        recordWriter.removeRecord(new UUIDKey(recordKey));
    }

    @Override
    public List<Bag> getBags(String instanceId) {
        return getBagProvider().getBags(instanceId);
    }

    @Override
    public Bag getBag(String instanceId, String name) {
        return getBagProvider().getBag(instanceId, name);
    }

    @Override
    public boolean containsBag(String instanceId, String bag) {
        return getBagProvider().containsBag(instanceId, bag);
    }

    @Override
    public void addBag(Bag bag) throws JasDBStorageException {
        getBagProvider().addBag(bag);
    }

    @Override
    public void removeBag(String instanceId, String name) throws JasDBStorageException {
        getBagProvider().removeBag(instanceId, name);
    }

    @Override
    public void addBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
        getBagProvider().addBagIndex(instanceId, bagName, indexDefinition);
    }

    @Override
    public void removeBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException {
        getBagProvider().removeBagIndex(instanceId, bagName, indexDefinition);
    }

    @Override
    public boolean containsIndex(String instanceId, String bagName, IndexDefinition indexDefinition) {
        Bag bag = getBag(instanceId, bagName);
        return bag.getIndexDefinitions().contains(indexDefinition);
    }

    @Override
    public List<Instance> getInstances() {
        return getInstanceProvider().getInstances();
    }

    @Override
    public Instance getInstance(String instanceId) {
        return getInstanceProvider().getInstance(instanceId);
    }

    @Override
    public boolean containsInstance(String instanceId) {
        return getInstanceProvider().containsInstance(instanceId);
    }

    @Override
    public Instance addInstance(String instanceId) throws JasDBStorageException {
        return getInstanceProvider().addInstance(instanceId);
    }

    @Override
    public void removeInstance(String instanceId) throws JasDBStorageException {
        getInstanceProvider().removeInstance(instanceId);
    }

    @Override
    public void updateInstance(Instance instance) throws JasDBStorageException {
        getInstanceProvider().updateInstance(instance);
    }

    @Override
    public List<Entity> getMetadataEntities() throws JasDBStorageException {
        return getEntityStream().collect(Collectors.toList());
    }

    @Override
    public List<Entity> getMetadataEntities(String metadataType) throws JasDBStorageException {
        return getEntityStream()
                .filter(e -> e.getValue(Constants.META_TYPE).toString().equalsIgnoreCase(metadataType))
                .collect(Collectors.toList());
    }

    private Stream<Entity> getEntityStream() throws JasDBStorageException {
        Spliterator<RecordResult> stream = Spliterators.spliteratorUnknownSize(recordWriter.readAllRecords(), Spliterator.ORDERED);
        return StreamSupport.stream(stream, false).map(r -> {
            RecordResultImpl recordResult = (RecordResultImpl) r;
            try {
                return SimpleEntity.fromStream(recordResult.getStream());
            } catch (JasDBStorageException e) {
                throw new RuntimeJasDBException("Unable to map entity", e);
            }
        });
    }
}
