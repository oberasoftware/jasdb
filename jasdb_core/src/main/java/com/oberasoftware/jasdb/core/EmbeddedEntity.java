package com.oberasoftware.jasdb.core;

/**
 * @author Renze de Vries
 */
public class EmbeddedEntity extends SimpleEntity {


    @Override
    public String getInternalId() {
        return null;
    }

    @Override
    public void setInternalId(String internalId) {
        //not implemented
    }
}
