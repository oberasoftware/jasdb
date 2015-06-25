package nl.renarj.jasdb.storage;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import static nl.renarj.jasdb.core.utils.FileUtils.deleteSafely;
import static nl.renarj.jasdb.core.utils.FileUtils.removeExtension;

/**
 * @author Renze de Vries
 */
@Component
@Singleton
public class RecordWriterFactoryLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RecordWriterFactoryLoader.class);

    private static final String BAG_EXTENSION = ".pjs";

    private static final String DEFAULT_PROVIDER = "transactional";

    private String recordWriterProvider = DEFAULT_PROVIDER;

    @Inject
    private MetadataStore metadataStore;

    private Map<String, RecordWriter> recordWriters = new ConcurrentHashMap<>();

    private RecordWriterFactory recordWriterFactory;

    @Inject
    public RecordWriterFactoryLoader(ConfigurationLoader configurationLoader) throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();
        Configuration recordWriterConfiguration = configuration.getChildConfiguration("/jasdb/Storage/RecordWriter");
        recordWriterProvider = recordWriterConfiguration != null ? recordWriterConfiguration.getAttribute("provider", DEFAULT_PROVIDER) : DEFAULT_PROVIDER;

        ServiceLoader<RecordWriterFactory> recordWriterFactories = ServiceLoader.load(RecordWriterFactory.class);

        for(RecordWriterFactory recordWriterFactory : recordWriterFactories) {
            if(recordWriterFactory.providerName().equals(recordWriterProvider)) {
                this.recordWriterFactory = recordWriterFactory;
                LOG.info("Using RecordWriterFactory: {}", recordWriterFactory);
            }
        }

        if(recordWriterFactory == null) {
            throw new ConfigurationException("No record writer factory is available, could not load configured provider: " + recordWriterProvider);
        }
    }

    @PreDestroy
    public void closeRecordWriters() throws JasDBStorageException {
        for(RecordWriter recordWriter : recordWriters.values()) {
            recordWriter.closeWriter();
        }
    }

    public RecordWriter loadRecordWriter(String instanceId, String bagName) throws JasDBStorageException {
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

    private synchronized RecordWriter loadExistingWriter(File file) throws JasDBStorageException {
        if(!recordWriters.containsKey(file.toString())) {
            RecordWriter recordWriter = recordWriterFactory.createWriter(file);
            recordWriter.openWriter();
            recordWriters.put(file.toString(), recordWriter);
        }
        return recordWriters.get(file.toString());
    }
}
