package com.obera.service.acl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.crypto.CryptoEngine;
import nl.renarj.jasdb.core.crypto.CryptoFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Singleton
public class SessionManagerImpl implements SessionManager {
    private UserManager userManager;

    private Map<String, SecureUserSession> secureUserSessionMap = new ConcurrentHashMap<String, SecureUserSession>();

    @Inject
    public SessionManagerImpl(UserManager userManager) {
        this.userManager = userManager;
    }

    @Override
    public UserSession startSession(Credentials credentials) throws JasDBStorageException {
        User user = userManager.authenticate(credentials);

        String sessionId = UUID.randomUUID().toString();
        String token = UUID.randomUUID().toString();

        CryptoEngine userEncryptionEngine = CryptoFactory.getEngine(user.getEncryptionEngine());
        String encryptedContentKey = user.getEncryptedContentKey();
        String contentKey = userEncryptionEngine.decrypt(user.getPasswordSalt(), credentials.getPassword(), encryptedContentKey);
        encryptedContentKey = userEncryptionEngine.encrypt(user.getPasswordSalt(), token, contentKey);

        UserSession session = new UserSessionImpl(sessionId, token, encryptedContentKey, user);
        userManager.authorize(session, "/", AccessMode.CONNECT);

        secureUserSessionMap.put(sessionId, new SecureUserSession(session));

        return session;
    }

    @Override
    public boolean sessionValid(String sessionId) throws JasDBStorageException {
        return secureUserSessionMap.containsKey(sessionId);
    }

    @Override
    public List<UserSession> getActiveSessions() throws JasDBStorageException {
        return new ArrayList<UserSession>(secureUserSessionMap.values());
    }

    @Override
    public void endSession(String sessionId) throws JasDBStorageException {
        secureUserSessionMap.remove(sessionId);
    }

    @Override
    public UserSession getSession(String sessionId) throws JasDBStorageException {
        return secureUserSessionMap.get(sessionId);
    }
}
