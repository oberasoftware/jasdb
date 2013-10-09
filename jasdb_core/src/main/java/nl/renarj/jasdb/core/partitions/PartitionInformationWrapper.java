/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.partitions;

import nl.renarj.jasdb.core.locator.NodeInformation;

/**
 * User: renarj
 * Date: 2/22/12
 * Time: 11:28 PM
 */
public class PartitionInformationWrapper {
    private NodeInformation nodeInformation;
    private BagPartition partition;
    private boolean local;
    
    public PartitionInformationWrapper(NodeInformation nodeInformation, BagPartition partition, boolean isLocal) {
        this.nodeInformation = nodeInformation;
        this.partition = partition;
        this.local = isLocal;
    }

    public String getPartitionId() {
        return partition.getPartitionId();
    }

    public NodeInformation getNodeInformation() {
        return nodeInformation;
    }

    public BagPartition getPartition() {
        return partition;
    }

    public boolean isLocal() {
        return local;
    }
    
    public boolean allowsWrite() {
        return getPartitionState().isWriteAllowed();
    }
    
    public boolean allowsRead() {
        return getPartitionState().isReadAllowed();
    }

    public boolean isComplete() {
        return getPartitionState().isComplete();
    }

    public PartitionStates getPartitionState() {
        return PartitionStates.fromString(partition.getStatus());
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Partition: ").append(partition);
        builder.append(", nodeInformation: ").append(nodeInformation);
        builder.append(", local: ").append(local);
        builder.append(", status: ").append(partition.getStatus());
        
        return builder.toString();
    }
}
