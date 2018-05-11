package com.oberasoftware.jasdb.cluster;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class PartitionConfig {
    private String instanceId;
    private String bag;

    private List<String> fields;

    public PartitionConfig(String instanceId, String bag, List<String> fields) {
        this.instanceId = instanceId;
        this.bag = bag;
        this.fields = fields;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getBag() {
        return bag;
    }

    public void setBag(String bag) {
        this.bag = bag;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "PartitionConfig{" +
                "instanceId='" + instanceId + '\'' +
                ", bag='" + bag + '\'' +
                ", fields=" + fields +
                '}';
    }
}
