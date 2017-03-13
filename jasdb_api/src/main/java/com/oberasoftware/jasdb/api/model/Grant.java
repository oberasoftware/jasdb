package com.oberasoftware.jasdb.api.model;

import com.oberasoftware.jasdb.api.security.AccessMode;

/**
 * @author Renze de Vries
 */
public interface Grant {
    AccessMode getAccessMode();

    String getGrantedUsername();
}
