package nl.renarj.jasdb.api.metadata;

import nl.renarj.jasdb.api.acl.AccessMode;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface GrantObject {
    String getObjectName();

    void addGrant(Grant grant);

    boolean isGranted(String userName, AccessMode mode);

    Grant getGrant(String userName);

    List<Grant> getGrants();

    void removeGrant(String userName);
}
