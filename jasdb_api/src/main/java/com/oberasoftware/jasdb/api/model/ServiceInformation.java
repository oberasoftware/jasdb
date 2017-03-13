package com.oberasoftware.jasdb.api.model;

import java.util.Map;

/**
 * @author Renze de Vries
 */
public class ServiceInformation {
    private Map<String, String> nodeProperties;
    private String serviceType;

    public ServiceInformation(String serviceType, Map<String, String> nodeProperties) {
        this.serviceType = serviceType;
        this.nodeProperties = nodeProperties;
    }

    public Map<String, String> getNodeProperties() {
        return nodeProperties;
    }

    public String getServiceType() {
        return serviceType;
    }
}
