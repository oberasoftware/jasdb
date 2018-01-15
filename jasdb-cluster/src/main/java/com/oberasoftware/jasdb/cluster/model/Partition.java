package com.oberasoftware.jasdb.cluster.model;

import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.SimpleEntity;

import java.io.Serializable;
import java.util.List;

/**
 * @author renarj
 */
public class Partition implements Serializable {
    private String instanceId;
    private String bag;

    private List<String> fields;

    private PartitionStatus status;
    private PartitionType type;

    private NodeInformation nodeInformation;

    private String partitionKey;


    public Partition(String instanceId, String bag) {
        this.instanceId = instanceId;
        this.bag = bag;
    }

    public static Partition fromEntity(Entity entity) {
        String instanceId = entity.getValue("instanceId");
        String bag = entity.getValue("bag");
        String key = entity.getValue("key");
        List<String> fields = entity.getValues("fields");
        PartitionStatus status = PartitionStatus.valueOf(entity.getValue("status"));
        PartitionType type = PartitionType.valueOf(entity.getValue("partitionType"));

        Partition partition = new Partition(instanceId, bag);
        partition.setFields(fields);
        partition.setPartitionKey(key);
        partition.setStatus(status);
        partition.setType(type);
        return partition;
    }

    public static Entity toEntity(Partition partition) {
        SimpleEntity entity = new SimpleEntity();
        entity.setProperty("instanceId", partition.getInstanceId());
        entity.setProperty("bag", partition.getBag());
        entity.setProperty("key", partition.getPartitionKey());
        entity.setProperty("fields", partition.getFields().toArray(new String[0]));
        entity.setProperty("status", partition.getStatus().toString());
        entity.setProperty("partitionType", partition.getType().toString());

        return entity;
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

    public PartitionStatus getStatus() {
        return status;
    }

    public void setStatus(PartitionStatus status) {
        this.status = status;
    }

    public PartitionType getType() {
        return type;
    }

    public void setType(PartitionType type) {
        this.type = type;
    }

    public NodeInformation getNodeInformation() {
        return nodeInformation;
    }

    public void setNodeInformation(NodeInformation nodeInformation) {
        this.nodeInformation = nodeInformation;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "instanceId='" + instanceId + '\'' +
                ", bag='" + bag + '\'' +
                ", fields=" + fields +
                ", status=" + status +
                ", type=" + type +
                ", nodeInformation=" + nodeInformation +
                ", partitionKey='" + partitionKey + '\'' +
                '}';
    }
}
