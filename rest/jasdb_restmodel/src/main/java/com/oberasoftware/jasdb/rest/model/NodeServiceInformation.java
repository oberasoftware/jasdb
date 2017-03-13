package com.oberasoftware.jasdb.rest.model;

import java.util.Map;

/**
 * @author Renze de Vries
 */
public class NodeServiceInformation {
    private Map<String, String> properties;
    private String serviceType;

    public NodeServiceInformation() {

    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
