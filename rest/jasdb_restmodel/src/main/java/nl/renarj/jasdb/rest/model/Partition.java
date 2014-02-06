/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.model;

/**
 * User: renarj
 * Date: 1/22/12
 * Time: 2:55 PM
 */
public class Partition implements RestEntity {
    private String bag;
    private String instance;
    private String partitionId;
    private String status;
    private String type;
    private String strategy;
    private String start;
    private String end;
    private long size;

    public Partition(String instance, String bag, String partitionId, String type, String strategy, String status, String start, String end, long size) {
        this.partitionId = partitionId;
        this.type = type;
        this.strategy = strategy;
        this.status = status;
        this.start = start;
        this.end = end;
        this.bag = bag;
        this.instance = instance;
        this.size = size;
    }

    public Partition() {

    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getBag() {
        return bag;
    }

    public void setBag(String bag) {
        this.bag = bag;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
