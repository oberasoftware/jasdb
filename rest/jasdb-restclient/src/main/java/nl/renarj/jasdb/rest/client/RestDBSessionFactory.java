package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
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
