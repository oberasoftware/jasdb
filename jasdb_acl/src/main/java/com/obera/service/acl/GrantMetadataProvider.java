package com.obera.service.acl;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.MetadataProvider;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.metadata.MetaWrapper;

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
    private Map<String, MetaWrapper<EncryptedGrants>> grantMetaMap = new ConcurrentHashMap<String, MetaWrapper<EncryptedGrants>>();

    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getMetadataType() {
        return GRANT_TYPE;
    }

    @Override
    public void registerMetadataEntity(SimpleEntity entity, long recordPointer) throws JasDBStorageException {
        EncryptedGrants encryptedGrants = EncryptedGrants.fromEntity(entity);
        grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<EncryptedGrants>(encryptedGrants, recordPointer));
    }

    public boolean hasGrant(String objectName) throws JasDBStorageException {
        return grantMetaMap.containsKey(objectName);
    }

    public List<EncryptedGrants> getGrants() throws JasDBStorageException {
        List<EncryptedGrants> grants = new ArrayList<EncryptedGrants>();
        for(MetaWrapper<EncryptedGrants> grantWrapper : grantMetaMap.values()) {
            grants.add(grantWrapper.getMetadataObject());
        }
        return grants;
    }

    public void persistGrant(EncryptedGrants encryptedGrants) throws JasDBStorageException {
        if(grantMetaMap.containsKey(encryptedGrants.getObjectName())) {
            MetaWrapper<EncryptedGrants> grantsMetaWrapper = grantMetaMap.get(encryptedGrants.getObjectName());
            long updatedPointer = metadataStore.updateMetadataEntity(EncryptedGrants.toEntity(encryptedGrants), grantsMetaWrapper.getRecordPointer());
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<EncryptedGrants>(encryptedGrants, updatedPointer));
        } else {
            long recordPointer = metadataStore.addMetadataEntity(EncryptedGrants.toEntity(encryptedGrants));
            grantMetaMap.put(encryptedGrants.getObjectName(), new MetaWrapper<EncryptedGrants>(encryptedGrants, recordPointer));
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
