package com.oberasoftware.jasdb.rest.model;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class RestGrantObjectCollection implements RestEntity {
    private List<RestGrantObject> grantList;

    public RestGrantObjectCollection(List<RestGrantObject> grantList) {
        this.grantList = grantList;
    }

    public RestGrantObjectCollection() {

    }

    public List<RestGrantObject> getGrantList() {
        return grantList;
    }

    public void setGrantList(List<RestGrantObject> grantList) {
        this.grantList = grantList;
    }
}
