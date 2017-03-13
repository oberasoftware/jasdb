package com.oberasoftware.jasdb.rest.client.legacy;

import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.rest.client.RestDBSession;

/**
 * Small wrapper class for backwards compatibility
 */
public class LocalRestClient extends RestDBSession {
    public LocalRestClient() throws JasDBStorageException {
        this("default");
    }

    public LocalRestClient(String instanceId) throws JasDBStorageException {
        super(instanceId, "127.0.0.1", 7050);
    }

    public LocalRestClient(Credentials credentials) throws JasDBStorageException {
        this("default", credentials);
    }

    public LocalRestClient(String instanceId, Credentials credentials) throws JasDBStorageException {
        super(instanceId, "127.0.0.1", credentials, 7050);
    }
}
