package com.oberasoftware.jasdb.api.session;

import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.Instance;

import java.util.List;

/**
 * This is the main entry point for the JasDB database operations. From this
 * point a bag or list of bags can be retrieved. Bags can also be removed using
 * this session API.
 *
 * Every session is bound to a specific instance, if no instance was specified during creation it will
 * refer to the default instance if present. If no default instance exists one can be created using
 * the addInstance operation on the session. The session can be switched to another instance using the
 * 'switchInstance' operation.
 *
 *
 * When the session is finished the resources used by the session can be cleaned
 * using the closeSession operation.
 *
 * @author Renze de Vries
 */
public interface DBSession {
    UserAdministration getUserAdministration() throws JasDBStorageException;

    /**
     * Gets a list of all available instances
     * @return The list of all available instances
     * @throws JasDBStorageException If unable to load the list of available instances
     */
    List<Instance> getInstances() throws JasDBStorageException;

    /**
     * Gets the metadata of a specific instance
     * @return The metadata of a specific instance
     * @throws JasDBStorageException If unable to load the metadata of the instance
     */
    Instance getInstance(String instanceId) throws JasDBStorageException;

    /**
     * Adds a new instance with the given path to the DB. The session still
     * is attached to the original connecting instance. @see switchInstance or @see addAndSwitchInstance
     * for switching to another instance.
     *
     * @param instanceId The id of the instance, should not exist already
     * @throws JasDBStorageException If unable to to add an instance
     */
    void addInstance(String instanceId) throws JasDBStorageException;

    /**
     * Adds a new instance with the given path to the DB. The session will
     * switch to the newly created instance.
     *
     * @param instanceId The id of the instance, should not exist already
     * @throws JasDBStorageException If unable to to add an instance
     */
    void addAndSwitchInstance(String instanceId) throws JasDBStorageException;

    /**
     * This deletes an instance all its related bags and entities permanently. This operation
     * cannot be rolled back and is permanent. If the resources are in use this operation will
     * fail.
     *
     * When the session is bound to the instance to be deleted the session switches to the default instance if present.
     *
     * @param instanceId The instanceId of the instance and related resources (bags, entities) to delete
     * @return Returns the now active session instance, in case the instance was deleted the session switches to the default instance.
     * @throws JasDBStorageException If unable to delete the instance and related resources (bags, entities)
     */
    Instance deleteInstance(String instanceId) throws JasDBStorageException;

    /**
     * Switches the current session to a new instance
     * @param instanceId The id of the instance
     * @throws JasDBStorageException If unable to switch to instance
     */
    void switchInstance(String instanceId) throws JasDBStorageException;

    /**
     * Gets the instance id of the currently connected instance
     * @return The id of the instance
     * @throws JasDBStorageException If unable to get the id of the current connected instance
     */
    String getInstanceId() throws JasDBStorageException;

    /**
     * Creates a bag or gets an existing one if it already existed for the connected instance.
     * In case the bag does not exist it will be created in the database.
     *
     * @param bagName The name of the bag
     * @return The created or already existing bag
     * @throws JasDBStorageException If unable to create or get the bag
     */
    EntityBag createOrGetBag(String bagName) throws JasDBStorageException;

    /**
     * Creates a bag or gets an existing one if it already existed for the specified instance.
     * In case the bag does not exist it will be created in the database.
     *
     * @param bagName The name of the bag
     * @param instanceId The id of the instance
     * @return The created or already existing bag
     * @throws JasDBStorageException If unable to create or get the bag
     */
    EntityBag createOrGetBag(String instanceId, String bagName) throws JasDBStorageException;

    /**
     * Retrieves an existing bag if this exists on the connected instance, in case the bag does not
     * exist the method will return null
     *
     * @param bagName The name of the bag
     * @return The bag if existing in the database, will return null if it does not exist
     * @throws JasDBStorageException If unable to retrieve the bag
     */
    EntityBag getBag(String bagName) throws JasDBStorageException;

    /**
     * Retrieves an existing bag if this exists on the specified instance, in case the bag does not
     * exist the method will return null
     *
     * @param instanceId The id of the instance
     * @param bagName The name of the bag
     * @return The bag if existing in the database, will return null if it does not exist
     * @throws JasDBStorageException If unable to retrieve the bag
     */
    EntityBag getBag(String instanceId, String bagName) throws JasDBStorageException;

    /**
     * Gets a list of all existing bags in the default database instance
     * @return The list of entity bags
     * @throws JasDBStorageException If unable to retrieve the list of entity bags
     */
    List<EntityBag> getBags() throws JasDBStorageException;

    /**
     * Gets a list of all existing bags in the specified database instance
     *
     * @param instanceId The id of the instance
     * @return The list of entity bags
     * @throws JasDBStorageException If unable to retrieve the list of entity bags
     */
    List<EntityBag> getBags(String instanceId) throws JasDBStorageException;

    /**
     * Gets the entity manager that can be used for entity persistence
     * @return The EntityManager
     */
    EntityManager getEntityManager();

    /**
     * Removes a bag from the current instance if it exists and is not in use
     * @param bagName The name of the bag
     * @throws JasDBStorageException If unable to remove the bag when it does not exist or the bag is in use
     */
    void removeBag(String bagName) throws JasDBStorageException;

    /**
     * Removes a bag from the specified instance if it exists and is not in use
     *
     * @param instanceId The id of the instance
     * @param bagName The name of the bag
     * @throws JasDBStorageException If unable to remove the bag when it does not exist or the bag is in use
     */
    void removeBag(String instanceId, String bagName) throws JasDBStorageException;

    /**
     * Closes the session and removes used resources
     * @throws JasDBStorageException if unable to cleanly close the session
     */
    void closeSession() throws JasDBStorageException;
}
