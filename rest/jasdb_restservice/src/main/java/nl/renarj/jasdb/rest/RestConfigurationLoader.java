package nl.renarj.jasdb.rest;

import com.oberasoftware.jasdb.engine.JasDBConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
public class RestConfigurationLoader {
    private static final Logger LOG = getLogger(RestConfigurationLoader.class);

    private static final RestConfigurationLoader INSTANCE = new RestConfigurationLoader();

    private boolean enabled;

    private RestConfigurationLoader() {
        try {
            nl.renarj.core.utilities.configuration.Configuration dbConfig = new JasDBConfigurationLoader().getConfiguration();
            nl.renarj.core.utilities.configuration.Configuration restConfig = dbConfig.getChildConfiguration("/jasdb/Services/Remote[@service='rest']");
            this.enabled = restConfig != null && restConfig.getAttribute("Enabled", false);
            LOG.info("Rest service set to enabled: {}", enabled);
        } catch (ConfigurationException e) {
            LOG.error("Unable to load JasDB configuration, rest service disabled", e);
            this.enabled = false;
        }
    }

    public static boolean isEnabled() {
        return INSTANCE.enabled;
    }
}
