package nl.renarj.jasdb.rest.mappers;

import nl.renarj.jasdb.api.metadata.Grant;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.rest.model.RestGrant;
import nl.renarj.jasdb.rest.model.RestGrantObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class GrantModelMapper {
    public static RestGrantObject map(GrantObject grantObject) {
        grantObject.getGrants();

        List<RestGrant> mappedGrants = new ArrayList<RestGrant>();
        for(Grant grant : grantObject.getGrants()) {
            mappedGrants.add(map(grantObject.getObjectName(), grant));
        }

        return new RestGrantObject(grantObject.getObjectName(), mappedGrants);
    }

    public static RestGrant map(String objectName, Grant grant) {
        return new RestGrant(grant.getGrantedUsername(), objectName, grant.getAccessMode());
    }
}
