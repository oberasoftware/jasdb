/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.model;

import java.util.List;

/**
 * User: renarj
 * Date: 1/22/12
 * Time: 2:56 PM
 */
public class PartitionCollection implements RestEntity {
    private List<Partition> partitions;

    public PartitionCollection(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public PartitionCollection() {

    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }
}
