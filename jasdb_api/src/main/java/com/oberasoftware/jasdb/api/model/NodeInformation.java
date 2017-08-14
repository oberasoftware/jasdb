/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: renarj
 * Date: 1/16/12
 * Time: 9:23 PM
 */
public class NodeInformation implements Serializable {
    private String instanceId;
    private String gridId;

    private Map<String, ServiceInformation> serviceInformationTypes = new HashMap<>();

    public NodeInformation(String instanceId, String gridId) {
        this.gridId = gridId;
        this.instanceId = instanceId;
    }

    public String getGridId() {
        return gridId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void addServiceInformation(ServiceInformation serviceInformation) {
        serviceInformationTypes.put(serviceInformation.getServiceType(), serviceInformation);
    }

    public List<ServiceInformation> getServiceInformationList() {
        return new ArrayList<>(serviceInformationTypes.values());
    }

    public ServiceInformation getServiceInformation(String type) {
        return serviceInformationTypes.get(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeInformation nodeInformation = (NodeInformation) o;

        return instanceId.equals(nodeInformation.instanceId) && gridId.equals(nodeInformation.gridId);

    }

    @Override
    public int hashCode() {
        int result = gridId.hashCode();
        result = 31 * result + instanceId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "NodeInformation{" +
                "gridId='" + gridId + '\'' +
                ", instanceId='" + instanceId + '\'' +
                '}';
    }
}
