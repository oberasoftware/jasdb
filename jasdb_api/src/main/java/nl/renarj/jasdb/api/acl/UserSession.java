package nl.renarj.jasdb.api.acl;

import nl.renarj.jasdb.api.metadata.User;

/**
 * @author Renze de Vries
 */
public interface UserSession {
    String getSessionId();

    String getAccessToken();

    String getEncryptedContentKey();

    User getUser();
}
