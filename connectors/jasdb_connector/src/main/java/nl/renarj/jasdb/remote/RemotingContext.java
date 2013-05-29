/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.context.RequestContext;

/**
 * @author Renze de Vries
 */
public class RemotingContext extends RequestContext {
    private static final boolean DEFAULT_SECURE = false;

    public RemotingContext() {
        super(true, DEFAULT_SECURE);
    }
    
    public RemotingContext(boolean isClient) {
        super(isClient, DEFAULT_SECURE);
    }

    public RemotingContext(boolean isClient, boolean isSecure) {
        super(isClient, isSecure);
    }
}
