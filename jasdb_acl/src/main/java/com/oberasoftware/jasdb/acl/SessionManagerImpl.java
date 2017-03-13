package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.security.SessionManager;
import com.oberasoftware.jasdb.api.security.UserManager;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.security.CryptoEngine;
import com.oberasoftware.jasdb.core.crypto.CryptoFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.security.UserSessionImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
@Component
@Scope("prototype")
public class SessionManagerImpl implements SessionManager {
    private Map<String, SecureUserSession> secureUserSessionMap = new ConcurrentHashMap<>();

    @Autowired
    private UserManager userManager;


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
