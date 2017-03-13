package com.oberasoftware.jasdb.api.security;

import com.oberasoftware.jasdb.api.model.User;

/**
 * @author Renze de Vries
 */
public interface UserSession {
    String getSessionId();

    String getAccessToken();

    String getEncryptedContentKey();

    User getUser();
}
