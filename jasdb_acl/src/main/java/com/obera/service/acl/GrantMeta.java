package com.obera.service.acl;

import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.metadata.Grant;

/**
 * @author Renze de Vries
 */
public class GrantMeta implements Grant {
    private String userName;
    private AccessMode accessMode;

    public GrantMeta(String userName, AccessMode accessMode) {
        this.userName = userName;
        this.accessMode = accessMode;
    }

    @Override
    public AccessMode getAccessMode() {
        return accessMode;
    }

    @Override
    public String getGrantedUsername() {
        return userName;
    }

    @Override
    public String toString() {
        return "GrantMeta{" +
                "userName='" + userName + '\'' +
                ", accessMode=" + accessMode +
                '}';
    }
}
