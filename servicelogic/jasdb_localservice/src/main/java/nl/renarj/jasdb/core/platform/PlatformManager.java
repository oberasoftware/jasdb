package nl.renarj.jasdb.core.platform;

import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;

/**
 * @author Renze de Vries
 */
public interface PlatformManager {
    boolean platformMatch();

    String getDefaultStorageLocation();

    String getProcessId();

    void initializePlatform() throws ConfigurationException;

    void shutdownPlatform() throws JasDBException;

    <T> T getComponent(Class<T> type);

    String getVersionData();
}
