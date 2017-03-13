package com.oberasoftware.jasdb.rest.client;

import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.session.DBSessionFactory;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;

/**
 * @author Renze de Vries
 */
public class RestDBSessionFactory implements DBSessionFactory {
    private String instanceId;
    private String hostname;
    private int port;

    private boolean validateCertificates = true;

    public RestDBSessionFactory() {

    }

    public RestDBSessionFactory(boolean validateCertificates) {
        this.validateCertificates = validateCertificates;
    }

    @Override
    public DBSession createSession() throws JasDBStorageException {
        return new RestDBSession(instanceId, hostname, port);
    }

    @Override
    public DBSession createSession(String instance) throws JasDBStorageException {
        return new RestDBSession(instance, hostname, port);
    }

    @Override
    public DBSession createSession(Credentials credentials) throws JasDBStorageException {
        return new RestDBSession(instanceId, hostname, credentials, port, validateCertificates);
    }

    @Override
    public DBSession createSession(String instance, Credentials credentials) throws JasDBStorageException {
        return new RestDBSession(instance, hostname, credentials, port, validateCertificates);
    }

    @Override
    public void shutdown() throws JasDBStorageException {
        RemoteConnectorFactory.shutdown();
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
