package com.obera.service.acl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.CredentialsProvider;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.Grant;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.crypto.CryptoEngine;
import nl.renarj.jasdb.core.crypto.CryptoFactory;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.metadata.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Singleton
public class UserManagerImpl implements UserManager {
    private static final Logger LOG = LoggerFactory.getLogger(UserManagerImpl.class);
    private CredentialsProvider credentialsProvider;

    private ConcurrentHashMap<String, GrantObject> cachedGrants = new ConcurrentHashMap<>();

    @Inject
    public UserManagerImpl(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public User authenticate(Credentials credentials) throws JasDBStorageException {
        return credentialsProvider.getUser(credentials.getUsername(), credentials.getSourceHost(), credentials.getPassword());
    }

    @Override
    public void authorize(UserSession userSession, String object, AccessMode mode) throws JasDBStorageException {
        StatRecord authRecord = StatisticsMonitor.createRecord("auth:object");
        try {
            if(userSession != null) {
                String userName = userSession.getUser().getUsername();
                boolean granted = checkGrantHierarchy(object, userSession, mode);
                LOG.debug("User: {} is privileged: {} on object: {}", userName, granted, object);
                if(!granted) {
                    throw new JasDBSecurityException("User: " + userName + " has insufficient privileges on object: " + object);
                }
            } else {
                throw new JasDBSecurityException("Unable to authorize user, no session");
            }
        } finally {
            authRecord.stop();
        }
    }

    @Override
    public List<String> getUsers(UserSession currentSession) throws JasDBStorageException {
        authorize(currentSession, "/Users", AccessMode.READ);
        return credentialsProvider.getUsers();
    }

    @Override
    public User addUser(UserSession currentSession, String userName, String allowedHost, String password) throws JasDBStorageException {
        authorize(currentSession, "/Users", AccessMode.WRITE);

        User currentUser = currentSession.getUser();
        CryptoEngine cryptoEngine = CryptoFactory.getEngine();
        String contentKey = cryptoEngine.decrypt(currentUser.getPasswordSalt(), currentSession.getAccessToken(), currentSession.getEncryptedContentKey());

        return credentialsProvider.addUser(userName, allowedHost, contentKey, password);
    }

    @Override
    public void grantUser(UserSession currentSession, String object, String userName, AccessMode mode) throws JasDBStorageException {
        authorize(currentSession, "/Grants", AccessMode.WRITE);

        try {
            GrantMetadataProvider grantMetadataProvider = getGrantProvider();
            if(grantMetadataProvider.hasGrant(object)) {
                GrantObject grantObject = getMutableGrantObject(currentSession, object);
                grantObject.addGrant(new GrantMeta(userName, mode));
                grantMetadataProvider.persistGrant(encryptGrants(grantObject, currentSession));
            } else {
                GrantObject grantObject = new GrantObjectMeta(object, new GrantMeta(userName, mode));
                grantMetadataProvider.persistGrant(encryptGrants(grantObject, currentSession));
            }
        } finally {
            cachedGrants.remove(object);
        }
    }

    @Override
    public void revoke(UserSession currentSession, String object, String userName) throws JasDBStorageException {
        authorize(currentSession, "/Grants", AccessMode.DELETE);

        GrantObject grantObject = getMutableGrantObject(currentSession, object);
        if(grantObject != null) {
            grantObject.removeGrant(userName);
            getGrantProvider().persistGrant(encryptGrants(grantObject, currentSession));
            cachedGrants.remove(object);
        } else {
            throw new JasDBSecurityException("Unable to revoke grant, no object: " + object + " was found with grantObject for user: " + userName);
        }
    }

    private EncryptedGrants encryptGrants(GrantObject grantObject, UserSession userSession) throws JasDBStorageException {
        CryptoEngine cryptoEngine = CryptoFactory.getEngine();
        String contentKey = CryptoFactory.getEngine().decrypt(userSession.getUser().getPasswordSalt(), userSession.getAccessToken(), userSession.getEncryptedContentKey());

        String salt = cryptoEngine.generateSalt();
        String unencryptedData = SimpleEntity.toJson(GrantObjectMeta.toEntity(grantObject));
        String encryptedData = cryptoEngine.encrypt(salt, contentKey, unencryptedData);

        return new EncryptedGrants(grantObject.getObjectName(), encryptedData, salt, cryptoEngine.getDescriptor());
    }

    @Override
    public void deleteUser(UserSession session, String userName) throws JasDBStorageException {
        authorize(session, "/Users", AccessMode.WRITE);

        credentialsProvider.deleteUser(userName);
    }

    private boolean checkGrantHierarchy(String objectName, UserSession userSession, AccessMode objectMode) throws JasDBStorageException {
        String userName = userSession.getUser().getUsername();
        LOG.debug("Checking grant hierarchy for: {} for user: {}", objectName, userName);
        //check root read access
        StringBuilder currentPath = new StringBuilder();
        currentPath.append(Constants.OBJECT_SEPARATOR);
        AccessMode grantedMode = getGrantedMode(currentPath.toString(), userSession);
        LOG.debug("Root access mode: {} for user: {}", grantedMode, userName);
        grantedMode = grantedMode == null ? AccessMode.NONE : grantedMode;

        String[] pathElements = objectName.replaceFirst(Constants.OBJECT_SEPARATOR, "").split(Constants.OBJECT_SEPARATOR);
        for(String pathElement : pathElements) {
            currentPath.append(pathElement);
            AccessMode mode = getGrantedMode(currentPath.toString(), userSession);
            if(mode != null) {
                grantedMode = mode;
                if(mode == AccessMode.NONE) {
                    break;
                }
            }

            currentPath.append(Constants.OBJECT_SEPARATOR);
        }
        LOG.debug("Grant level: {} for path: {}", grantedMode, currentPath.toString());
        boolean granted = grantedMode != null ? grantedMode.getRank() >= objectMode.getRank() : false;

        return granted;
    }

    @Override
    public GrantObject getGrantObject(UserSession session, String object) throws JasDBStorageException {
        authorize(session, "/Grants", AccessMode.READ);

        GrantObject grantObject = getMutableGrantObject(session, object);
        if(grantObject != null) {
            return new ImmutableGrantObject(grantObject);
        } else {
            return null;
        }
    }

    @Override
    public List<GrantObject> getGrantObjects(UserSession session) throws JasDBStorageException {
        List<EncryptedGrants> encryptedGrants = getGrantProvider().getGrants();
        List<GrantObject> grantObjects = new ArrayList<>();
        for(EncryptedGrants encryptedGrant : encryptedGrants) {
            grantObjects.add(decrypt(session, encryptedGrant));
        }
        return grantObjects;
    }

    private GrantObject getMutableGrantObject(UserSession userSession, String object) throws JasDBStorageException {
        EncryptedGrants encryptedGrants = getGrantProvider().getObjectGrants(object);

        if(encryptedGrants != null) {
            StatRecord getGrantRecord = StatisticsMonitor.createRecord("auth:grant:load");
            try {
                GrantObject grantObject = decrypt(userSession, encryptedGrants);
                LOG.debug("Found grantObject for object: {}, grantObject: {}", object, grantObject);
                return grantObject;
            } finally {
                getGrantRecord.stop();
            }
        } else {
            LOG.debug("No grants found for object: {}", object);
            return null;
        }
    }

    private GrantObject decrypt(UserSession session, EncryptedGrants encryptedGrants) throws JasDBStorageException {
        CryptoEngine contentCryptoEngine = CryptoFactory.getEngine();

        String contentKey = contentCryptoEngine.decrypt(session.getUser().getPasswordSalt(), session.getAccessToken(), session.getEncryptedContentKey());

        CryptoEngine cryptoEngine = CryptoFactory.getEngine(encryptedGrants.getEncryptionEngine());
        String decryptedData = cryptoEngine.decrypt(encryptedGrants.getSalt(), contentKey, encryptedGrants.getEncryptedData());

        return GrantObjectMeta.fromEntity(SimpleEntity.fromJson(decryptedData));
    }

    private GrantMetadataProvider getGrantProvider() throws JasDBStorageException {
        MetadataStore metadataStore = SimpleKernel.getMetadataStore();
        return metadataStore.getMetadataProvider(GrantMetadataProvider.GRANT_TYPE);
    }

    private AccessMode getGrantedMode(String objectName, UserSession userSession) throws JasDBStorageException {
        StatRecord getGrantRecord = StatisticsMonitor.createRecord("auth:grant:check");
        try {
            String username = userSession.getUser().getUsername();
            if(cachedGrants.containsKey(objectName)) {
                return verifyGrantMode(cachedGrants.get(objectName), username);
            } else {
                GrantObject objectGrantObject = getMutableGrantObject(userSession, objectName);
                if(objectGrantObject != null) {
                    cachedGrants.put(objectName, objectGrantObject);
                    return verifyGrantMode(objectGrantObject, username);
                } else {
                    return null;
                }
            }
        } finally {
            getGrantRecord.stop();
        }
    }

    private AccessMode verifyGrantMode(GrantObject grantObject, String username) {
        if(grantObject != null) {
            Grant userGrant = grantObject.getGrant(username);
            return userGrant != null ? userGrant.getAccessMode() : null;
        } else {
            return null;
        }

    }
}
