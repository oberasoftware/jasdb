package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.session.Entity;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 * @author Renze de Vries
 */
public interface MetadataStore {
    /**
     * Close the store
     * @throws JasDBStorageException If unable to close the store cleanly
     */
    void closeStore() throws JasDBStorageException;

    File getDatastoreLocation();

    /**
     * Checks whether the last shutdown was done cleanly
     * @return True if the last shutdown was clean, False if not
     * @throws JasDBStorageException If unable to determine if last shutdown is clean
     */
    boolean isLastShutdownClean() throws JasDBStorageException;

    /**
     * Generic operation to add a metadata entity to the metadata store
     * @param entity The metadata entity to add
     * @return The metadata key
     * @throws JasDBStorageException If unable to add the metadata entity
     */
    UUID addMetadataEntity(Entity entity) throws JasDBStorageException;

    /**
     * Generic operation to update a metadata entity in the metadata store
     * @param entity The updated entity
     * @return The metadata key
     * @throws JasDBStorageException If unable to update the entity
     */
    UUID updateMetadataEntity(Entity entity) throws JasDBStorageException;

    /**
     * Deletes a generic metadata entity
     * @param metadataKey The key of the metadata to delete
     * @throws JasDBStorageException If unable to delete the entity
     */
    void deleteMetadataEntity(UUID metadataKey) throws JasDBStorageException;

    /**
     * Gets the bags for the given instanceId
     * @param instanceId The instanceId
     * @return The list of bags for that instance
     * @throws JasDBStorageException If unable to load the bags
     */
    List<Bag> getBags(String instanceId) throws JasDBStorageException;

    /**
     * Gets the a specific bag in an instance
     * @param instanceId The instanceId
     * @param bag The name of the bag
     * @return The bag if present, null if not
     * @throws JasDBStorageException If unable to load the bag
     */
    Bag getBag(String instanceId, String bag) throws JasDBStorageException;

    /**
     * Checks if a bag in an instance exists in the metadata store
     * @param instanceId The instance in which the bag should exist
     * @param bag The bag that should exist
     * @return True if the bag exists in the instance, False if not
     * @throws JasDBStorageException If unable to check if the bag exists in the instance
     */
    boolean containsBag(String instanceId, String bag) throws JasDBStorageException;

    /**
     * Adds a new bag to the storage
     * @param bag The bag to add
     * @throws JasDBStorageException If unable to add the bag
     */
    void addBag(Bag bag) throws JasDBStorageException;

    /**
     * Adds an index to an existing bag in a certain instance
     *
     * @param instanceId The instance that contains the bag
     * @param bagName The bag to add the index to
     * @param indexDefinition The index definition
     * @throws JasDBStorageException If unable to add the index
     */
    void addBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException;

    /**
     * @param instanceId The instance that contains the bag
     * @param bagName The bag to remove the index from
     * @param indexDefinition The index definition
     * @throws JasDBStorageException If unable to remove the index
     */
    void removeBagIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException;

    /**
     * Checks if the index is already present in the metadata store
     * @param instanceId The instance that contains the bag
     * @param bagName The bag to check the index presence on
     * @param indexDefinition The index definition
     * @return True if the index is already present, False if not
     * @throws JasDBStorageException If unable to check if the index is present
     */
    boolean containsIndex(String instanceId, String bagName, IndexDefinition indexDefinition) throws JasDBStorageException;

    /**
     * Remove a bag from the metadata store
     * @param instanceId The name of the instance holding the bag
     * @param name The name of the bag
     * @throws JasDBStorageException If unable to remove the bag
     */
    void removeBag(String instanceId, String name) throws JasDBStorageException;

    /**
     * Gets the registered instances
     * @return The list of known instances
     * @throws JasDBStorageException If unable to get the list of instances
     */
    List<Instance> getInstances() throws JasDBStorageException;

    /**
     * Gets a specific instance identified by the instanceId
     * @param instanceId The instanceId
     * @return The instance if found, null if not
     * @throws JasDBStorageException If unable to get the instance
     */
    Instance getInstance(String instanceId) throws JasDBStorageException;

    /**
     * Checks if the instance alredy exists
     * @param instanceId The instanceId
     * @return True if the instance exists, False if not
     * @throws JasDBStorageException If unable to check if the instance exists
     */
    boolean containsInstance(String instanceId) throws JasDBStorageException;

    /**
     * Adds an instance using the specified instance metadata
     * @param instanceId The instance
     * @throws JasDBStorageException If unable to add the instance
     */
    Instance addInstance(String instanceId) throws JasDBStorageException;

    /**
     * Removes an instance from the metadata list
     * @param instanceId The instance to remove
     * @throws JasDBStorageException If unable to remove the instance
     */
    void removeInstance(String instanceId) throws JasDBStorageException;

    /**
     * Updates the instance metadata
     * @param instance The instance
     * @throws JasDBStorageException If unable to update the instance
     */
    void updateInstance(Instance instance) throws JasDBStorageException;

    /**
     * Gets a list of metadata entities from the metadata store
     * @return The list of metadata entities
     * @throws JasDBStorageException If unable to retrieve the list of metadata instances
     */
    List<Entity> getMetadataEntities() throws JasDBStorageException;

    /**
     * Gets a list of metadata entities from the metadata store with a specific type
     * @param metadataType The type of metadata entities you want to retrieve
     * @return The list of metadata entities of the specified type
     * @throws JasDBStorageException If unable to retrieve the list of metadata instances
     */
    List<Entity> getMetadataEntities(String metadataType) throws JasDBStorageException;

}
