package com.oberasoftware.jasdb.rest.model;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class RestGrantObject implements RestEntity {
    private List<RestGrant> grants;
    private String objectName;

    public RestGrantObject(String objectName, List<RestGrant> grants) {
        this.grants = grants;
        this.objectName = objectName;
    }

    public RestGrantObject() {

    }

    public List<RestGrant> getGrants() {
        return grants;
    }

    public void setGrants(List<RestGrant> grants) {
        this.grants = grants;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }
}
