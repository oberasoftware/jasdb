package nl.renarj.jasdb.core.utils;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.PlatformManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * @author Renze de Vries
 */
public class HomeLocatorUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HomeLocatorUtil.class);

    private static final String JASDB_DEFAULT_FOLDER = ".jasdb";
    public static final String JASDB_HOME = "JASDB_HOME";

    public static File determineDatastoreLocation() throws JasDBStorageException {
        Map<String, String> environmentVariables = System.getenv();
        String storeLocation;
        if(environmentVariables.containsKey(JASDB_HOME)) {
            storeLocation = environmentVariables.get(JASDB_HOME);
            LOG.info("Using Environment variable JASDB_HOME: {}", storeLocation);
        } else if(System.getProperty(JASDB_HOME) != null) {
            storeLocation = System.getProperty(JASDB_HOME);
            LOG.info("Using JASDB_HOME: {}", storeLocation);
        } else {
            storeLocation = PlatformManagerFactory.getPlatformManager().getDefaultStorageLocation();
            LOG.info("Getting platform directory: {}", storeLocation);
        }

        File datastoreLocation = new File(storeLocation, JASDB_DEFAULT_FOLDER);
        if(!datastoreLocation.exists() && !datastoreLocation.mkdirs()) {
            throw new JasDBStorageException("Unable to create JasDB home directory: " + datastoreLocation.toString());
        }
        return datastoreLocation;
    }


}
