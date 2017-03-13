package com.oberasoftware.jasdb.api.exceptions;

/**
 * When the configuation file could not be found this exception is thrown
 *
 * @author Renze de Vries
 */
public class ConfigurationNotFoundException extends CoreConfigException {
    public ConfigurationNotFoundException(String message, Throwable e) {
        super(message, e);
    }

    public ConfigurationNotFoundException(String message) {
        super(message);
    }
}
