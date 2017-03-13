package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.engine.metadata.MetaWrapper;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.engine.MetadataProvider;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

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
    public void registerMetadataEntity(Entity entity, long recordPointer) throws JasDBStorageException {
        User user = UserMeta.fromEntity(entity);
        userMetaMap.put(user.getUsername(), new MetaWrapper<>(user, recordPointer));
    }
}
