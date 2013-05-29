/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service.partitioning;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.core.partitions.PartitionInformationWrapper;
import nl.renarj.jasdb.service.IdGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * User: renarj
 * Date: 1/19/12
 * Time: 9:20 AM
 */
public class LocalPartitionManager implements PartitioningManager {
    public static final String LOCAL_STRATEGY = "LocalPartition";
    private static final String START_RANGE = "0";
    private static final String END_RANGE = "F";

    private MetadataStore metadataStore;
    private IdGenerator generator;

    public LocalPartitionManager(IdGenerator generator, MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.generator = generator;
    }

    @Override
    public void configure(Configuration configuration) throws ConfigurationException {
    }

    @Override
    public void initializePartitions() throws JasDBStorageException {
//        if(bagInfoReaderWriter.getPartitions().isEmpty()) {
//            BagPartition partition = new BagPartition(generator.generateNewId(), LOCAL_STRATEGY, "local", "ok", START_RANGE, END_RANGE, 0);
//            if(!bagInfoReaderWriter.containsPartition(partition)) {
//                bagInfoReaderWriter.createOrUpdate(partition);
//            }
//        }
    }

    @Override
    public List<BagPartition> splitPartition(BagPartition partition) throws JasDBStorageException {
        throw new JasDBStorageException("Split Operation not supported on local storage");
    }

    @Override
    public boolean changePartitionStatus(String partitionId, String targetState) throws JasDBStorageException {
        throw new JasDBStorageException("Changing of partition status not supported on local storage");
    }

    @Override
    public List<PartitionInformationWrapper> getKnownPartitions() throws JasDBStorageException {
        List<PartitionInformationWrapper> wrappers = new ArrayList<PartitionInformationWrapper>();
//        for(BagPartition partition : bagInfoReaderWriter.getPartitions()) {
//            PartitionInformationWrapper wrapper = new PartitionInformationWrapper(SimpleKernel.getNodeInformation(), partition,true);
//            wrappers.add(wrapper);
//        }
        return wrappers;
    }

    @Override
    public BagPartition getLocalPartition(String partitionId) throws JasDBStorageException {
//        return bagInfoReaderWriter.getPartitionById(partitionId);
        return null;
    }
}
