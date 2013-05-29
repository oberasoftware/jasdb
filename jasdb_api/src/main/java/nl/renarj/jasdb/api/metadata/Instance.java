package nl.renarj.jasdb.api.metadata;

/**
 * This represents an Instance in Jasdb, every JasDB can have multiple instances. Each instance is a unit which can
 * contain bags which can have indexes and stored data entities.
 *
 * @author Renze de Vries
 */
public interface Instance {
    /**
     * Gets the identifier of this instance
     * @return The identifier of this instance
     */
    String getInstanceId();

    /**
     * The storage path used for this instance
     * @return The storage path used for this instance
     */
    String getPath();
}
