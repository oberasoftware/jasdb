package nl.renarj.jasdb.rest;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.ServiceException;
import nl.renarj.jasdb.core.locator.GridLocatorUtil;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import nl.renarj.jasdb.core.utils.FileUtils;
import nl.renarj.jasdb.rest.security.OAuthTokenEndpoint;
import nl.renarj.jasdb.rest.security.OAuthTokenFilter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

@Service
@Singleton
public class RestService implements RemoteService {
    private static final Logger LOG = LoggerFactory.getLogger(RestService.class);

    private static final String REST_SSL_PORT_PATH = "/jasdb/Services/Remote[@service='rest']/Property[@Name='sslPort']";
    private static final String REST_SSL_KEYSTORE_PATH = "/jasdb/Services/Remote[@service='rest']/Property[@Name='sslKeystore']";
    private static final String REST_SSL_KEYSTORE_PASS_PATH = "/jasdb/Services/Remote[@service='rest']/Property[@Name='sslKeystorePassword']";
    private static final String PROPERTY_VALUE = "Value";
    private static final String REST_CONFIG_PATH = "/jasdb/Services/Remote[@service='rest']";
    private static final String REST_PORT_CONFIG = "port";
    private static final String REST_OAUTH_ENABLED = "oauth";
    private static final boolean DEFAULT_OAUTH = false;

    private static final int DEFAULT_PORT = 7050;

    private ServiceInformation serviceInformation;

	private int portNr;
    private SSLDetails sslDetails;
    private boolean oauthEnabled;

	private Server server;

    private boolean restEnabled = false;

    @Autowired
    private ConfigurationLoader configurationLoader;
    
    public RestService() throws ConfigurationException {
	}

    @PostConstruct
    public void init() throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();

        Configuration restConfiguration = configuration.getChildConfiguration(REST_CONFIG_PATH);
        restEnabled = restConfiguration != null && restConfiguration.getAttribute("Enabled", false);
        if(restEnabled) {
            LOG.info("Rest service is enabled, configuring");
            portNr = restConfiguration.getAttribute(REST_PORT_CONFIG, DEFAULT_PORT);
            oauthEnabled = restConfiguration.getAttribute(REST_OAUTH_ENABLED, DEFAULT_OAUTH);

            loadNodeData();
            sslDetails = loadSSLDetails(configuration);
        }
    }

    private SSLDetails loadSSLDetails(Configuration configuration) throws ConfigurationException {
        Configuration sslPortConfiguration = configuration.getChildConfiguration(REST_SSL_PORT_PATH);
        Configuration sslKeystoreConfig = configuration.getChildConfiguration(REST_SSL_KEYSTORE_PATH);
        Configuration sslKeystorePasswordConfig = configuration.getChildConfiguration(REST_SSL_KEYSTORE_PASS_PATH);

        if(sslPortConfiguration != null && sslKeystoreConfig != null && sslKeystorePasswordConfig != null) {
            return new SSLDetails(sslPortConfiguration.getAttribute(PROPERTY_VALUE, 7051),
                    sslKeystoreConfig.getAttribute(PROPERTY_VALUE),
                    sslKeystorePasswordConfig.getAttribute(PROPERTY_VALUE));
        }
        return null;
    }
    
    private void loadNodeData() throws ConfigurationException {
        try {
            String address = GridLocatorUtil.getPublicAddress().getHostAddress();

            Map<String, String> serviceProperties = new HashMap<>();
            serviceProperties.put("connectorType", "rest");
            serviceProperties.put("protocol", "http");
            serviceProperties.put("host", address);
            serviceProperties.put("port", "" + portNr);

            this.serviceInformation = new ServiceInformation("rest", serviceProperties);
        } catch(ConfigurationException e) {
            throw new ConfigurationException("Unable to load public endpoint information", e);
        }
    }

    @Override
    public boolean isEnabled() {
        return restEnabled;
    }

    @Override
    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }

    @Override
    public void startService() throws JasDBException {
        if(restEnabled) {
            server = new Server(portNr);

            if(sslDetails != null) {
                String keystorePath = FileUtils.resolveResourcePath(sslDetails.getKeystore());
                LOG.info("Using keystore path: {}", keystorePath);
                SslContextFactory sslContextFactory = new SslContextFactory(keystorePath);
                sslContextFactory.setKeyStorePassword(sslDetails.getKeystorePass());

//                Connector connector = new SslSelectChannelConnector(sslContextFactory);
//                connector.setPort(sslDetails.getSslPort());
//                server.addConnector(connector);
//                LOG.info("Starting SSL connector: {}", sslDetails);
            }

            ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);

            contextHandler.setContextPath("/");
            server.setHandler(contextHandler);

            if(oauthEnabled) {
                ServletHolder oauthTokenEndpoint = new ServletHolder(OAuthTokenEndpoint.class);
                contextHandler.addServlet(oauthTokenEndpoint, "/token");
                contextHandler.addFilter(OAuthTokenFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
            }

            ServletHolder holder = new ServletHolder(ServletContainer.class);
            holder.setInitParameter("javax.ws.rs.Application", "nl.renarj.jasdb.rest.JasdbRestApplication");
            contextHandler.addServlet(holder, "/*");

            try {
                LOG.info("Starting Jetty Rest service on port: {}", portNr);
                server.start();
            } catch(Exception e) {
                LOG.error("Unable to start nodeInformation", e);
            }
        } else {
            LOG.info("Rest service is not enabled or not configured, skipping start");
        }
	}
    
    @Override
    public void stopService() throws ServiceException {
		if(restEnabled && server != null) {
			LOG.info("Stopping Rest service");
			try {
				server.stop();
			} catch(Exception e) {
				LOG.error("Unable to stop service cleanly: " + e.getMessage());
			}
		}
	}
}
