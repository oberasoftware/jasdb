package com.oberasoftware.jasdb.rest.model.mappers;

import com.oberasoftware.jasdb.api.model.Grant;
import com.oberasoftware.jasdb.api.model.GrantObject;
import com.oberasoftware.jasdb.rest.model.RestGrant;
import com.oberasoftware.jasdb.rest.model.RestGrantObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class GrantModelMapper {
    public static RestGrantObject map(GrantObject grantObject) {
        grantObject.getGrants();

        List<RestGrant> mappedGrants = new ArrayList<>();
        for(Grant grant : grantObject.getGrants()) {
            mappedGrants.add(map(grantObject.getObjectName(), grant));
        }

        return new RestGrantObject(grantObject.getObjectName(), mappedGrants);
    }

    public static RestGrant map(String objectName, Grant grant) {
        return new RestGrant(grant.getGrantedUsername(), objectName, grant.getAccessMode());
    }
}
