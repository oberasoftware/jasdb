/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.context;

import nl.renarj.jasdb.api.acl.UserSession;

/**
 * User: renarj
 * Date: 1/22/12
 * Time: 3:24 PM
 */
public class RequestContext {
    private UserSession userSession;
    private boolean clientRequest;
    private boolean secure;

    public RequestContext(boolean clientRequest, boolean secure) {
        this.clientRequest = clientRequest;
        this.secure = secure;
    }

    public boolean isSecure() {
        return secure;
    }

    public UserSession getUserSession() {
        return userSession;
    }

    public void setUserSession(UserSession userSession) {
        this.userSession = userSession;
    }

    public boolean isClientRequest() {
        return clientRequest;
    }
}
