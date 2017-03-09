package com.oberasoftware.jasdb.acl;

import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.metadata.User;

/**
 * @author Renze de Vries
 */
public class UserSessionImpl implements UserSession {
    private String sessionId;
    private String accessToken;
    private String encryptedContentKey;
    private User user;

    public UserSessionImpl(String sessionId, String accessToken, String encryptedContentKey, User user) {
        this.sessionId = sessionId;
        this.accessToken = accessToken;
        this.user = user;
        this.encryptedContentKey = encryptedContentKey;
    }

    @Override
    public String getEncryptedContentKey() {
        return encryptedContentKey;
    }

    @Override
    public User getUser() {
        return user;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public String getAccessToken() {
        return accessToken;
    }

    @Override
    public String toString() {
        return "UserSessionImpl{" +
                "sessionId='" + sessionId + '\'' +
                ", user=" + user +
                '}';
    }
}
