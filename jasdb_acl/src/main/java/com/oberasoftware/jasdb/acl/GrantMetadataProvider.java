package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.engine.metadata.MetaWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
public class GrantMetadataProvider implements MetadataProvider {
    public static final String GRANT_TYPE = "grantMetadata";

    private MetadataStore metadataStore;
    private Map<String, MetaWrapper<EncryptedGrants>> grantMetaMap = new ConcurrentHashMap<>();

    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getMetadataType() {
        return GRANT_TYPE;
    }

    @Override
    public void registerMetadataEntity(Entity entity, long recordPointer) throws JasDBStorageException {
        EncryptedGrants encryptedGrants = EncryptedGrants.fromEntity(entity);
        grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, recordPointer));
    }

    public boolean hasGrant(String objectName) throws JasDBStorageException {
        return grantMetaMap.containsKey(objectName);
    }

    public List<EncryptedGrants> getGrants() throws JasDBStorageException {
        List<EncryptedGrants> grants = new ArrayList<>();
        for(MetaWrapper<EncryptedGrants> grantWrapper : grantMetaMap.values()) {
            grants.add(grantWrapper.getMetadataObject());
        }
        return grants;
    }

    public void persistGrant(EncryptedGrants encryptedGrants) throws JasDBStorageException {
        if(grantMetaMap.containsKey(encryptedGrants.getObjectName())) {
            MetaWrapper<EncryptedGrants> grantsMetaWrapper = grantMetaMap.get(encryptedGrants.getObjectName());
            long updatedPointer = metadataStore.updateMetadataEntity(EncryptedGrants.toEntity(encryptedGrants), grantsMetaWrapper.getRecordPointer());
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, updatedPointer));
        } else {
            long recordPointer = metadataStore.addMetadataEntity(EncryptedGrants.toEntity(encryptedGrants));
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<>(encryptedGrants, recordPointer));
        }
    }

    public EncryptedGrants getObjectGrants(String objectName) throws JasDBStorageException {
        if(grantMetaMap.containsKey(objectName)) {
            return grantMetaMap.get(objectName).getMetadataObject();
        } else {
            return null;
        }
    }
}
