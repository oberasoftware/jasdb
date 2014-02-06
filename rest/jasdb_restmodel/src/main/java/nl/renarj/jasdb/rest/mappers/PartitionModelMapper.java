/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.mappers;

import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.rest.model.Partition;
import nl.renarj.jasdb.rest.model.PartitionCollection;
import nl.renarj.jasdb.rest.model.RestBag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: renarj
 * Date: 3/22/12
 * Time: 8:19 PM
 */
public class PartitionModelMapper {
    public static PartitionCollection map(Collection<BagPartition> partitions, RestBag bag) {
        return map(partitions, bag.getInstanceId(), bag.getName());
    }

    public static PartitionCollection map(Collection<BagPartition> partitions, String instance, String bag) {
        List<Partition> mappedPartitions = new ArrayList<>(partitions.size());

        for(BagPartition partition : partitions) {
            mappedPartitions.add(map(partition, instance, bag));
        }

        return new PartitionCollection(mappedPartitions);
    }

    public static BagPartition map(Partition partition) {
        return new BagPartition(partition.getPartitionId(), partition.getStrategy(), partition.getType(), partition.getStatus(), partition.getStart(), partition.getEnd(), partition.getSize());
    }

    public static Partition map(BagPartition partition, RestBag bag) {
        return map(partition, bag.getInstanceId(), bag.getName());
    }

    public static Partition map(BagPartition partition, String instance, String bag) {
        return new Partition(instance, bag, partition.getPartitionId(), partition.getPartitionType(), partition.getPartitionStrategy(), partition.getStatus(), partition.getStart(), partition.getEnd(), partition.getSize());
    }
}
