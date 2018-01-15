package com.oberasoftware.jasdb.cluster.metadata;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.engine.metadata.BagMeta;
import com.oberasoftware.jasdb.engine.metadata.Constants;
import com.oberasoftware.jasdb.engine.metadata.MetaWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;

@Component
public class PartitionMetadataProvider implements MetadataProvider {

    public static String PARTITION_TYPE = "partitionMetaData";

    @Autowired
    private MetadataStore metadataStore;

    @PostConstruct
    public void loadData() throws JasDBStorageException {
        List<Entity> entities = metadataStore.getMetadataEntities(PARTITION_TYPE);

        for(Entity entity: entities) {
            UUID metaKey = UUID.fromString(entity.getInternalId());


        }
    }


    @Override
    public String getMetadataType() {
        return PARTITION_TYPE;
    }

}
