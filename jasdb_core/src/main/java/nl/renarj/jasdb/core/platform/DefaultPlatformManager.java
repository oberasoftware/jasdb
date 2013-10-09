package nl.renarj.jasdb.core.platform;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public class DefaultPlatformManager implements PlatformManager {
    private String platformId = "" + System.currentTimeMillis();

    @Override
    public boolean platformMatch(String platformName) {
        return true;
    }

    @Override
    public String getDefaultStorageLocation() {
        return System.getProperty("user.home");
    }

    @Override
    public String getProcessId() {
        return platformId;
    }

    @Override
    public void initializePlatform() throws JasDBStorageException {

    }

    @Override
    public void shutdownPlatform() throws JasDBStorageException {

    }

    @Override
    public String getVersionData() {
        return "Unknown";
    }
}
