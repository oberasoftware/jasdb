package com.oberasoftware.jasdb.api.engine;

import java.util.List;

/**
 * @author renarj
 */
public interface Configuration {
    String getAttribute(String attributeName);

    String getAttribute(String attributeName, String defaultValue);

    int getAttribute(String attributeName, int defaultValue);

    boolean getAttribute(String attributeName, boolean defaultValue);

    boolean hasAttribute(String attributeName);

    String getName();

    Configuration getChildConfiguration(String configurationPath);

    List<Configuration> getChildConfigurations(String configurationPath);

    List<Configuration> getChildren();
}
