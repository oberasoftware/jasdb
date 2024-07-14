package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.engine.ConfigurationLoader;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.api.storage.RecordWriterFactory;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import static com.oberasoftware.jasdb.core.utils.FileUtils.deleteSafely;
import static com.oberasoftware.jasdb.core.utils.FileUtils.removeExtension;

/**
 * @author Renze de Vries
 */
@Component
public class RecordWriterFactoryLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RecordWriterFactoryLoader.class);

    private static final String BAG_EXTENSION = ".pjs";

    private static final String DEFAULT_PROVIDER = "transactional";

    private final MetadataStore metadataStore;

    private Map<String, RecordWriter<UUIDKey>> recordWriters = new ConcurrentHashMap<>();

    private RecordWriterFactory<UUIDKey> recordWriterFactory;

    @Autowired
    public RecordWriterFactoryLoader(ConfigurationLoader configurationLoader, MetadataStore metadataStore) throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();
        Configuration recordWriterConfiguration = configuration.getChildConfiguration("/jasdb/Storage/RecordWriter");
        String recordWriterProvider = recordWriterConfiguration != null ? recordWriterConfiguration.getAttribute("provider", DEFAULT_PROVIDER) : DEFAULT_PROVIDER;

        ServiceLoader<RecordWriterFactory> recordWriterFactories = ServiceLoader.load(RecordWriterFactory.class);

        for(RecordWriterFactory recordWriterFactory : recordWriterFactories) {
            if(recordWriterFactory.providerName().equals(recordWriterProvider)) {
                this.recordWriterFactory = (RecordWriterFactory<UUIDKey>)recordWriterFactory;
                LOG.info("Using RecordWriterFactory: {}", recordWriterFactory);
            }
        }

        if(recordWriterFactory == null) {
            throw new ConfigurationException("No record writer factory is available, could not load configured provider: " + recordWriterProvider);
        }
        this.metadataStore = metadataStore;
    }

    @PreDestroy
    public void closeRecordWriters() throws JasDBStorageException {
        for(RecordWriter recordWriter : recordWriters.values()) {
            recordWriter.closeWriter();
        }
    }

    public RecordWriter<UUIDKey> loadRecordWriter(String instanceId, String bagName) throws JasDBStorageException {
        File filePath = getWriterPath(instanceId, bagName);
        String fileKey = filePath.toString();
        if(recordWriters.containsKey(fileKey)) {
            return recordWriters.get(fileKey);
        } else {
            return loadExistingWriter(filePath);
        }
    }

    public void remove(String instanceId, String bagName) throws JasDBStorageException {
        File writerPath = getWriterPath(instanceId, bagName);
        RecordWriter recordWriter = recordWriters.get(writerPath.toString());
        recordWriter.closeWriter();

        File indexLocation = new File(removeExtension(writerPath.toString()) + ".idx");
        deleteSafely(indexLocation);
        deleteSafely(writerPath);

        recordWriters.remove(writerPath.toString());
    }

    private File getWriterPath(String instanceId, String bagName) throws JasDBStorageException {
        String instancePath = metadataStore.getInstance(instanceId).getPath();
        return new File(instancePath, bagName + BAG_EXTENSION);
    }

    private synchronized RecordWriter<UUIDKey> loadExistingWriter(File file) throws JasDBStorageException {
        if(!recordWriters.containsKey(file.toString())) {
            RecordWriter<UUIDKey> recordWriter = recordWriterFactory.createWriter(file);
            recordWriter.openWriter();
            recordWriters.put(file.toString(), recordWriter);
        }
        return recordWriters.get(file.toString());
    }
}
