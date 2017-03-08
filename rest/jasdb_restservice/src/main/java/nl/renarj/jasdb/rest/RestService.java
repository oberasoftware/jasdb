package nl.renarj.jasdb.rest;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.ServiceException;
import nl.renarj.jasdb.core.locator.GridLocatorUtil;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Service
public class RestService implements RemoteService {
    private static final Logger LOG = LoggerFactory.getLogger(RestService.class);

    private static final String REST_CONFIG_PATH = "/jasdb/Services/Remote[@service='rest']";
    private static final String REST_OAUTH_ENABLED = "oauth";
    private static final boolean DEFAULT_OAUTH = false;
    private static final int DEFAULT_PORT = 7050;

    private ServiceInformation serviceInformation;

    private boolean oauthEnabled = false;

    private int portNr;

    private final ConfigurationLoader configurationLoader;
    
    @Autowired
    public RestService(ConfigurationLoader configurationLoader) throws ConfigurationException {
        this.configurationLoader = configurationLoader;
    }

    @PostConstruct
    public void init() throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();

        Configuration restConfiguration = configuration.getChildConfiguration(REST_CONFIG_PATH);
        LOG.info("Rest service is enabled, configuring");
        if(restConfiguration != null) {
            oauthEnabled = restConfiguration.getAttribute(REST_OAUTH_ENABLED, DEFAULT_OAUTH);
        }
        portNr = getRestPort(configurationLoader);

        loadNodeData();
    }

    @Bean
    @Autowired
    public EmbeddedServletContainerCustomizer container(ConfigurationLoader configurationLoader) {
        return (container -> {
            int port = getRestPort(configurationLoader);
            LOG.info("Setting rest port to: {}", port);
            container.setPort(port);
        });
    }

    private int getRestPort(ConfigurationLoader configurationLoader) {
        try {
            LOG.info("Determining port");
            Configuration dbConfig = configurationLoader.getConfiguration();
            Configuration restConfig = dbConfig.getChildConfiguration("/jasdb/Services/Remote[@service='rest']");

            if(restConfig != null) {
                return restConfig.getAttribute("port", DEFAULT_PORT);
            } else {
                return DEFAULT_PORT;
            }
        } catch(ConfigurationException e) {
            LOG.info("Could not load configuration assuming default port: {}", DEFAULT_PORT);
            return DEFAULT_PORT;
        }
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
        return true;
    }

    @Override
    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }

    @Override
    public void startService() throws JasDBException {
	}
    
    @Override
    public void stopService() throws ServiceException {
	}
}
