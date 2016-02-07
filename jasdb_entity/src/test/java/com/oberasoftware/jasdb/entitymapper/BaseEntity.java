package com.oberasoftware.jasdb.entitymapper;

import com.oberasoftware.jasdb.api.entitymapper.annotations.JasDBProperty;

/**
 * @author Renze de Vries
 */
public class BaseEntity {
    private String emailAddress;

    public BaseEntity(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public BaseEntity() {
    }

    @JasDBProperty
    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }
}
