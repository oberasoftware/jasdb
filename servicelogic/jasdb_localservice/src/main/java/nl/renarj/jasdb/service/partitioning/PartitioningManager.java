package nl.renarj.jasdb.service.partitioning;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.core.partitions.PartitionInformationWrapper;

import java.util.List;

/**
 * User: renarj
 * Date: 1/18/12
 * Time: 9:31 PM
 */
public interface PartitioningManager {
    public void configure(Configuration configuration) throws ConfigurationException;
    
    public void initializePartitions() throws JasDBStorageException;

    public List<BagPartition> splitPartition(BagPartition partition) throws JasDBStorageException;

    public boolean changePartitionStatus(String partitionId, String targetState) throws JasDBStorageException;
    
    public List<PartitionInformationWrapper> getKnownPartitions() throws JasDBStorageException;
    
    public BagPartition getLocalPartition(String partitionId) throws JasDBStorageException;
}
