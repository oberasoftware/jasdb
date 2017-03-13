package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.security.CryptoEngine;
import com.oberasoftware.jasdb.core.crypto.CryptoFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;

/**
 * @author Renze de Vries
 */
public class SecureUserSession implements UserSession {
    private String sessionId;
    private String encryptedContentKey;
    private User user;
    private String accessTokenHash;

    public SecureUserSession(UserSession userSession) {
        this.sessionId = userSession.getSessionId();
        this.user = userSession.getUser();
        this.encryptedContentKey = userSession.getEncryptedContentKey();

        try {
            CryptoEngine cryptoEngine = CryptoFactory.getEngine();
            accessTokenHash = cryptoEngine.hash(sessionId, userSession.getAccessToken());
        } catch(JasDBSecurityException e) {
            throw new RuntimeJasDBException("Unable to hash token", e);
        }
    }

    @Override
    public String getEncryptedContentKey() {
        return encryptedContentKey;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public String getAccessToken() {
        return accessTokenHash;
    }

    @Override
    public User getUser() {
        return user;
    }
}
