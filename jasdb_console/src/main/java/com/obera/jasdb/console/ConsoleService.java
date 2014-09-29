package com.obera.jasdb.console;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.GridLocatorUtil;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * @author renarj
 */
@Component
public class ConsoleService implements RemoteService {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleService.class);

    private static final String CONSOLE_CONFIG_PATH = "/jasdb/Services/Remote[@service='console']";
    private static final String CONSOLE_PORT_CONFIG = "port";

    private static final int DEFAULT_PORT = 7052;

    private Server server;

    private int portNr = DEFAULT_PORT;

    private ServiceInformation serviceInformation;

    private boolean consoleEnabled = false;

    @Autowired
    private ConfigurationLoader configurationLoader;

    public ConsoleService() {
    }

    @PostConstruct
    public void init() throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();

        Configuration consoleConfiguration = configuration.getChildConfiguration(CONSOLE_CONFIG_PATH);
        consoleEnabled = consoleConfiguration != null && consoleConfiguration.getAttribute("Enabled", false);
        if(consoleEnabled) {
            LOG.info("Console UI service is enabled, configuring");
            portNr = consoleConfiguration.getAttribute(CONSOLE_PORT_CONFIG, DEFAULT_PORT);

            loadNodeData();
        }
    }

    private void loadNodeData() throws ConfigurationException {
        try {
            String address = GridLocatorUtil.getPublicAddress().getHostAddress();

            Map<String, String> serviceProperties = new HashMap<>();
            serviceProperties.put("connectorType", "console");
            serviceProperties.put("protocol", "http");
            serviceProperties.put("host", address);
            serviceProperties.put("port", "" + portNr);

            this.serviceInformation = new ServiceInformation("console", serviceProperties);
        } catch(ConfigurationException e) {
            throw new ConfigurationException("Unable to load public endpoint information", e);
        }
    }


    @Override
    public boolean isEnabled() {
        return consoleEnabled;
    }

    @Override
    public void startService() throws JasDBException {
        if(consoleEnabled) {
            LOG.info("Starting Web console");
            server = new Server(portNr);

            WebAppContext app = new WebAppContext();
            app.setContextPath("/");
            try {
                app.setWar(new ClassPathResource("webapp").getURI().toString());

                server.setHandler(app);

                LOG.info("Starting Console service on port: {}", portNr);
                server.start();
            } catch (Exception e) {
                throw new JasDBException("Unable to start Console UI service", e);
            }
        }
    }

    @Override
    public void stopService() throws JasDBException {
        if(consoleEnabled && server != null) {
            LOG.info("Stopping Console UI service");
            try {
                server.stop();
            } catch(Exception e) {
                LOG.error("Unable to stop service cleanly: " + e.getMessage());
            }
        }

    }

    @Override
    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }
}
