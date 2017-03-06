package nl.renarj.jasdb.api;

import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * The DBInstanceFactory manages all active and configured database instances
 */
public interface DBInstanceFactory {

    /**
     * Gets the default Database instance
     * @return The default database instance
     * @throws ConfigurationException If unable to configure
     */
	DBInstance getInstance() throws ConfigurationException;

    /**
     * Gets the instance for the specified instance id
     * @param instanceName The name of the instance
     * @return The instance specified, or null if does not exist
     * @throws ConfigurationException If unable to load the instance
     */
	DBInstance getInstance(String instanceName) throws ConfigurationException;

    /**
     * Returns wether an instanceId exists or not
     * @param instanceId The instanceId to check for
     * @return True if the instance exists, False if not
     */
    boolean hasInstance(String instanceId);

    /**
     * Retrieves a list of all the instances configured
     * @return The list of all the instances
     */
	List<DBInstance> listInstances();

    /**
     * Does a shutdown of all the instances
     * @throws ConfigurationException
     */
	void shutdown() throws ConfigurationException;

    /**
     * Adds a new instance with default configuration with a specified
     * database path
     * @param instanceId The instanceId
     * @throws ConfigurationException If unable to configure the instance
     */
    void addInstance(String instanceId) throws JasDBStorageException;

    /**
     * Deletes an instance and all of its resources, this operation is destructive and
     * will remove all data permanently.
     *
     * @param instanceId The instance id to delete all resources for
     * @throws ConfigurationException If unable to remove the instance
     */
    void deleteInstance(String instanceId) throws JasDBStorageException;
}
