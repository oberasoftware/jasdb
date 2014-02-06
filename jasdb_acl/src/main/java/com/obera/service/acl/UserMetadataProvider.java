package com.obera.service.acl;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.MetadataProvider;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.metadata.MetaWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
public class UserMetadataProvider implements MetadataProvider {
    public static final String USERMETA_TYPE = "userMetadata";

    private MetadataStore metadataStore;

    private Map<String, MetaWrapper<User>> userMetaMap = new ConcurrentHashMap<>();

    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public User getUser(String username) throws JasDBStorageException {
        if(userMetaMap.containsKey(username)) {
            return userMetaMap.get(username).getMetadataObject();
        } else {
            throw new JasDBSecurityException("Invalid user credentials");
        }
    }

    public boolean hasUser(String username) {
        return userMetaMap.containsKey(username);
    }

    public List<User> getUsers() throws JasDBStorageException {
        List<User> users = new ArrayList<>();
        for(MetaWrapper<User> user : userMetaMap.values()) {
            users.add(user.getMetadataObject());
        }
        return users;
    }

    public void addUser(User user) throws JasDBStorageException {
        SimpleEntity entity = UserMeta.toEntity(user);
        long recordPointer = metadataStore.addMetadataEntity(entity);
        userMetaMap.put(user.getUsername(), new MetaWrapper<>(user, recordPointer));
    }

    public void delUser(String username) throws JasDBStorageException {
        MetaWrapper<User> userWrapper = userMetaMap.get(username);
        if(userWrapper != null) {
            metadataStore.deleteMetadataEntity(userWrapper.getRecordPointer());
            userMetaMap.remove(username);
        } else {
            throw new JasDBSecurityException("Unable to delete user, not found");
        }
    }

    @Override
    public String getMetadataType() {
        return USERMETA_TYPE;
    }

    @Override
    public void registerMetadataEntity(SimpleEntity entity, long recordPointer) throws JasDBStorageException {
        User user = UserMeta.fromEntity(entity);
        userMetaMap.put(user.getUsername(), new MetaWrapper<>(user, recordPointer));
    }
}
