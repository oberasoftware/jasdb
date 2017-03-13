package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.model.Grant;

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
