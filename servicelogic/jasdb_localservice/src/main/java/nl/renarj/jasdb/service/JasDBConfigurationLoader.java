package nl.renarj.jasdb.service;

import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
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
                this.configuration = Configuration.loadConfiguration(JASDB_CONFIG);
            } else {
                LOG.info("Override configuration path specified: {}", overrideConfigProperty);
                this.configuration = Configuration.loadConfiguration(overrideConfigProperty);
            }
        } catch (CoreConfigException e) {
            try {
                this.configuration = Configuration.loadConfiguration(FALLBACK_JASDB_XML);
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
