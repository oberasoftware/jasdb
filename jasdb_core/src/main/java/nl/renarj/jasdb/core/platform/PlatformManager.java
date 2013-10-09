package nl.renarj.jasdb.core.platform;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface PlatformManager {
    boolean platformMatch(String platformName);

    String getDefaultStorageLocation();

    String getProcessId();

    void initializePlatform() throws JasDBStorageException;

    void shutdownPlatform() throws JasDBStorageException;

    String getVersionData();
}
