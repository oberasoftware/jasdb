package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.api.engine.ConfigurationLoader;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.core.utils.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Component
public class JasDBConfigurationLoader implements ConfigurationLoader {
    private static final Logger LOG = LoggerFactory.getLogger(JasDBConfigurationLoader.class);

    private static final String FALLBACK_JASDB_XML = "default-jasdb.xml";
    private static final String JASDB_CONFIG = "jasdb.xml";

    private Configuration configuration;

    public JasDBConfigurationLoader() throws ConfigurationException {
        try {
            String overrideConfigProperty = System.getProperty("jasdb-config");
            if(StringUtils.stringEmpty(overrideConfigProperty)) {
                this.configuration = XMLConfiguration.loadConfiguration(JASDB_CONFIG);
            } else {
                LOG.info("Override configuration path specified: {}", overrideConfigProperty);
                this.configuration = XMLConfiguration.loadConfiguration(overrideConfigProperty);
            }
        } catch (CoreConfigException e) {
            try {
                this.configuration = XMLConfiguration.loadConfiguration(FALLBACK_JASDB_XML);
            } catch(CoreConfigException ex) {
                throw new ConfigurationException("Unable to load Default JasDB configuration file", ex);
            }
        }
    }

    @Override
    public Configuration getConfiguration() throws ConfigurationException {
        return configuration;
    }
}
