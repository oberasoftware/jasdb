package nl.renarj.jasdb.core.utils;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.io.File;
import java.util.Map;

/**
 * @author Renze de Vries
 */
public class HomeLocatorUtil {
    private static final String JASDB_DEFAULT_FOLDER = ".jasdb";
    public static final String JASDB_HOME = "JASDB_HOME";

    public static File determineDatastoreLocation() throws JasDBStorageException {
        Map<String, String> environmentVariables = System.getenv();
        String storeLocation;
        if(environmentVariables.containsKey(JASDB_HOME)) {
            storeLocation = environmentVariables.get(JASDB_HOME);
        } else if(System.getProperty(JASDB_HOME) != null) {
            storeLocation = System.getProperty(JASDB_HOME);
        } else {
            storeLocation = System.getProperty("user.home");
        }

        File datastoreLocation = new File(storeLocation, JASDB_DEFAULT_FOLDER);
        if(!datastoreLocation.exists() && !datastoreLocation.mkdirs()) {
            throw new JasDBStorageException("Unable to create JasDB home directory: " + datastoreLocation.toString());
        }
        return datastoreLocation;
    }


}
