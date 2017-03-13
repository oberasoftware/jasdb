package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.model.Grant;
import com.oberasoftware.jasdb.api.model.GrantObject;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class ImmutableGrantObject implements GrantObject {
    private GrantObject grantObject;

    public ImmutableGrantObject(GrantObject grantObject) {
        this.grantObject = grantObject;
    }

    @Override
    public String getObjectName() {
        return grantObject.getObjectName();
    }

    @Override
    public void addGrant(Grant grant) {
        throw new RuntimeJasDBException("Grant not allowed to be modified");
    }

    @Override
    public boolean isGranted(String userName, AccessMode mode) {
        return grantObject.isGranted(userName, mode);
    }

    @Override
    public Grant getGrant(String userName) {
        return grantObject.getGrant(userName);
    }

    @Override
    public List<Grant> getGrants() {
        return grantObject.getGrants();
    }

    @Override
    public void removeGrant(String userName) {
        throw new RuntimeJasDBException("Grant not allowed to be modified");
    }
}
