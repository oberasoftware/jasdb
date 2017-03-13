package com.oberasoftware.jasdb.rest.model;


import com.oberasoftware.jasdb.api.security.AccessMode;

/**
 * @author Renze de Vries
 */
public class RestGrant implements RestEntity {
    private String username;
    private String objectName;
    private AccessMode mode;

    public RestGrant(String username, String object, AccessMode mode) {
        this.username = username;
        this.objectName = object;
        this.mode = mode;
    }

    public RestGrant() {

    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public AccessMode getMode() {
        return mode;
    }

    public void setMode(AccessMode mode) {
        this.mode = mode;
    }
}
