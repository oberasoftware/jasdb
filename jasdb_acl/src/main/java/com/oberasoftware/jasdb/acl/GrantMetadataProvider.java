package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.engine.metadata.MetaWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Component
public class GrantMetadataProvider implements MetadataProvider {
    public static final String GRANT_TYPE = "grantMetadata";

    @Autowired
    private MetadataStore metadataStore;

    private Map<String, MetaWrapper<EncryptedGrants>> grantMetaMap = new ConcurrentHashMap<>();

    @Override
    public String getMetadataType() {
        return GRANT_TYPE;
    }

    @PostConstruct
    public void loadData() throws JasDBStorageException {
        List<Entity> entities = metadataStore.getMetadataEntities(GRANT_TYPE);

        for(Entity entity: entities) {
            UUID metaKey = UUID.fromString(entity.getInternalId());
            EncryptedGrants encryptedGrants = EncryptedGrants.fromEntity(entity);
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, metaKey));
        }
    }


    public boolean hasGrant(String objectName) {
        return grantMetaMap.containsKey(objectName);
    }

    public List<EncryptedGrants> getGrants() {
        List<EncryptedGrants> grants = new ArrayList<>();
        for(MetaWrapper<EncryptedGrants> grantWrapper : grantMetaMap.values()) {
            grants.add(grantWrapper.getMetadataObject());
        }
        return grants;
    }

    public void persistGrant(EncryptedGrants encryptedGrants) throws JasDBStorageException {
        if(grantMetaMap.containsKey(encryptedGrants.getObjectName())) {
            MetaWrapper<EncryptedGrants> grantsMetaWrapper = grantMetaMap.get(encryptedGrants.getObjectName());
            UUID updatedPointer = metadataStore.updateMetadataEntity(EncryptedGrants.toEntity(encryptedGrants));
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, updatedPointer));
        } else {
            UUID recordPointer = metadataStore.addMetadataEntity(EncryptedGrants.toEntity(encryptedGrants));
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, recordPointer));
        }
    }

    public EncryptedGrants getObjectGrants(String objectName) {
        if(grantMetaMap.containsKey(objectName)) {
            return grantMetaMap.get(objectName).getMetadataObject();
        } else {
            return null;
        }
    }
}
