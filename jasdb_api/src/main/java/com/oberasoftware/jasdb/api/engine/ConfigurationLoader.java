package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;

/**
 * @author Renze de Vries
 */
public interface ConfigurationLoader {
    Configuration getConfiguration() throws ConfigurationException;
}
