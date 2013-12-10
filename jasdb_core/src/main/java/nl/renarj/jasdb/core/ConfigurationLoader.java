package nl.renarj.jasdb.core;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;

/**
 * @author Renze de Vries
 */
public interface ConfigurationLoader {
    Configuration getConfiguration() throws ConfigurationException;
}
