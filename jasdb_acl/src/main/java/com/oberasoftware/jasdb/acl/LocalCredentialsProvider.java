package com.oberasoftware.jasdb.acl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.engine.metadata.Constants;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.security.CredentialsProvider;
import com.oberasoftware.jasdb.api.model.Grant;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.security.CryptoEngine;
import com.oberasoftware.jasdb.core.crypto.CryptoFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Renze de Vries
 */
@Component
@Scope("prototype")
public class LocalCredentialsProvider implements CredentialsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LocalCredentialsProvider.class);

    private UserMetadataProvider userMetadataProvider;
    private GrantMetadataProvider grantMetadataProvider;

    @Autowired
    private MetadataStore metadataStore;

    @PostConstruct
    public void initialize() throws JasDBStorageException {
        userMetadataProvider = metadataStore.getMetadataProvider(UserMetadataProvider.USERMETA_TYPE);
        grantMetadataProvider = metadataStore.getMetadataProvider(GrantMetadataProvider.GRANT_TYPE);

        //Checks if there is at least a basic user present, if not creates one
        if(userMetadataProvider.getUsers().isEmpty()) {
            createMandatoryAdminUser();
        }
    }

    private void createMandatoryAdminUser() throws JasDBStorageException {
        CryptoEngine cryptoEngine = CryptoFactory.getEngine();
        String salt = cryptoEngine.generateSalt();
        String contentKey = cryptoEngine.generateSalt();
        String encryptedContentKey = cryptoEngine.encrypt(salt, "", contentKey);

        User user = new UserMeta("admin", "localhost", encryptedContentKey, salt, cryptoEngine.hash(salt, ""), cryptoEngine.getDescriptor());
        userMetadataProvider.addUser(user);

        Grant grant = new GrantMeta("admin", AccessMode.ADMIN);
        GrantObjectMeta grantsMeta = new GrantObjectMeta(Constants.OBJECT_SEPARATOR, grant);
        String unencryptedGrants = SimpleEntity.toJson(GrantObjectMeta.toEntity(grantsMeta));

        String encryptedGrants = cryptoEngine.encrypt(salt, contentKey, unencryptedGrants);
        EncryptedGrants grants = new EncryptedGrants(grantsMeta.getObjectName(), encryptedGrants, salt, cryptoEngine.getDescriptor());
        grantMetadataProvider.persistGrant(grants);
    }

    @Override
    public User getUser(String userName, String sourceHost, String password) throws JasDBStorageException {
        User user = userMetadataProvider.getUser(userName);
        LOG.debug("Expected host: {} actual: {}", user.getHost(), sourceHost);
        CryptoEngine cryptoEngine = CryptoFactory.getEngine(user.getEncryptionEngine());
        if(user.getPasswordHash().equals(cryptoEngine.hash(user.getPasswordSalt(), password)) && (user.getHost().equals("*") || user.getHost().equals(sourceHost))) {
            LOG.debug("User: {} has been authenticated", user);
            return user;
        } else {
            throw new JasDBSecurityException("Could not authenticate, invalid credentials");
        }
    }

    @Override
    public void deleteUser(String userName) throws JasDBStorageException {
        userMetadataProvider.delUser(userName);
    }

    @Override
    public List<String> getUsers() throws JasDBStorageException {
        return Lists.transform(userMetadataProvider.getUsers(), new Function<User, String>() {
            @Override
            public String apply(User user) {
                return user.getUsername();
            }
        });
    }

    @Override
    public User addUser(String userName, String allowedHost, String contentKey, String password) throws JasDBStorageException {
        if(!userMetadataProvider.hasUser(userName)) {
            CryptoEngine cryptoEngine = CryptoFactory.getEngine();
            String salt = cryptoEngine.generateSalt();
            String encryptedContentKey = cryptoEngine.encrypt(salt, password, contentKey);
            String passwordHash = cryptoEngine.hash(salt, password);

            User user = new UserMeta(userName, allowedHost, encryptedContentKey, salt, passwordHash, cryptoEngine.getDescriptor());
            userMetadataProvider.addUser(user);
            return user;
        } else {
            return userMetadataProvider.getUser(userName);
        }
    }
}
