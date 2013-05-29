package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface PartitionConnector extends RemoteConnector {
    /* All partition retrieval and operations */
    QueryResult getPartitionEntities(RemotingContext context, String instance, String bag, String partitionId, int start, int limit) throws RemoteException;

    List<BagPartition> getPartitions(RemotingContext context, String instance, String bag) throws RemoteException;

    BagPartition getPartition(RemotingContext context, String instance, String bag, String partitionId) throws RemoteException;

    List<BagPartition> doPartitionOperation(RemotingContext context, String instance, String bag, String partitionId, String operation) throws RemoteException;
}
