package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.core.partitions.BagPartition;

import java.util.Set;

/**
 * This is used to retrieve information about a specific bag in an instance. It provides information about
 * the stored indexes and partitions available in the bag.
 *
 * User: renarj
 * Date: 1/5/12
 * Time: 9:50 PM
 */
public interface BagInfoReader {
    public Set<IndexDefinition> getIndexes();

    public Set<BagPartition> getPartitions();
    
    public BagPartition getPartitionById(String partitionId);
}
