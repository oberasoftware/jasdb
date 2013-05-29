package nl.renarj.jasdb.api.model;

import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * The DBInstanceFactory manages all active and configured database instances
 */
public interface DBInstanceFactory {
    void initializeServices(KernelContext kernelContext) throws JasDBStorageException;

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
     * @param path The path where the database is located
     * @throws ConfigurationException If unable to configure the instance
     */
    void addInstance(String instanceId, String path) throws JasDBStorageException;

    /**
     * Deletes an instance and all of its resources, this operation is destructive and
     * will remove all data permanently.
     *
     * @param instanceId The instance id to delete all resources for
     * @throws ConfigurationException If unable to remove the instance
     */
    void deleteInstance(String instanceId) throws JasDBStorageException;
}
