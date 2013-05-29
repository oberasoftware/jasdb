package nl.renarj.jasdb.api.metadata;

import nl.renarj.jasdb.api.acl.AccessMode;

/**
 * @author Renze de Vries
 */
public interface Grant {
    AccessMode getAccessMode();

    String getGrantedUsername();
}
