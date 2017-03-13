/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.rest.client;

import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import nl.renarj.jasdb.api.DBConnectorSession;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.api.model.ServiceInformation;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.TokenConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * @author  Renze de Vries
 */
public class RestDBSession extends DBConnectorSession {
    private static final Logger LOG = LoggerFactory.getLogger(RestDBSession.class);
    private static final String clientId = ManagementFactory.getRuntimeMXBean().getName().replace(".", "_");

    private RemotingContext context;

    public RestDBSession(String instance, String hostname, int port) throws JasDBStorageException {
        super(instance, createNodeInformation(hostname, false, false, port));
    }

    public RestDBSession(String instance, String hostname, Credentials credentials, int sslPort) throws JasDBStorageException {
        super(instance, credentials, createNodeInformation(hostname, true, true, sslPort));
    }

    public RestDBSession(String instance, String hostname, Credentials credentials, int sslPort, boolean verifyCertificate) throws JasDBStorageException {
        super(instance, credentials, createNodeInformation(hostname, true, verifyCertificate, sslPort));
    }

    @Override
    public EntityManager getEntityManager() {
//        return new EntityManagerImpl(this);
        return null;
    }

    @Override
    protected void authenticate(Credentials credentials) throws JasDBStorageException {
        if(credentials != null) {
            TokenConnector tokenConnector = RemoteConnectorFactory.createConnector(getNodeInformation(), TokenConnector.class);
            UserSession session = tokenConnector.loadSession(credentials.getUsername(), credentials.getPassword());

            if(StringUtils.stringNotEmpty(session.getAccessToken()) && StringUtils.stringNotEmpty(session.getSessionId())) {
                context = new RemotingContext(true);
                context.setUserSession(session);
                LOG.debug("Token: {} session: {}", session.getAccessToken(), session.getSessionId());
            } else {
                throw new JasDBSecurityException("Unable to obtain access token to service");
            }
        } else {
            context = new RemotingContext(true);
        }
    }

    @Override
    protected RemotingContext getContext() {
        return context;
    }

    public static NodeInformation createNodeInformation(String hostname, boolean ssl, boolean verifyCert, int port) {
        Map<String, String> serviceDetails = new HashMap<>();
        serviceDetails.put("connectorType", "rest");
        String protocol = "http";
        if(ssl) {
            protocol = "https";
        }
        serviceDetails.put("verifyCert", Boolean.toString(verifyCert));
        serviceDetails.put("protocol", protocol);
        serviceDetails.put("host", hostname);
        serviceDetails.put("port", "" + port);
        serviceDetails.put("clientid", clientId);

        NodeInformation nodeInformation = new NodeInformation("", "");
        nodeInformation.addServiceInformation(new ServiceInformation("rest", serviceDetails));
        return nodeInformation;
    }
}
