/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.rest.model;

import java.util.List;

/**
 * User: renarj
 * Date: 4/16/12
 * Time: 7:13 PM
 */
public class Node implements RestEntity {
    private String instanceId;
    private String gridId;
    private List<NodeServiceInformation> services;

    public Node() {

    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getGridId() {
        return gridId;
    }

    public void setGridId(String gridId) {
        this.gridId = gridId;
    }

    public List<NodeServiceInformation> getServices() {
        return services;
    }

    public void setServices(List<NodeServiceInformation> services) {
        this.services = services;
    }
}
