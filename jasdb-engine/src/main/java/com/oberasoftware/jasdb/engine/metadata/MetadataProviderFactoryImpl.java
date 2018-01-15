package com.oberasoftware.jasdb.engine.metadata;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataProviderFactory;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.writer.transactional.RecordIteratorImpl;
import com.oberasoftware.jasdb.writer.transactional.RecordResultImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
public class MetadataProviderFactoryImpl implements MetadataProviderFactory {

    @Autowired
    private MetadataStore metadataStore;

    @Autowired(required = false)
    private List<MetadataProvider> providers;

    @Override
    public <T extends MetadataProvider> T getProvider(String type) {
        T provider = (T) providers.stream().filter(p -> p.getMetadataType().equalsIgnoreCase(type))
                .findFirst()
                .orElseThrow(() -> new RuntimeJasDBException("Could not find metadata provider for: " + type));

        return provider;
    }
}
