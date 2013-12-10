package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.remote.PartitionConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.mappers.PartitionModelMapper;
import nl.renarj.jasdb.rest.model.Partition;
import nl.renarj.jasdb.rest.model.PartitionCollection;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 */
public class PartitionRestConnector extends RemoteRestConnector implements PartitionConnector {
    public PartitionRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public List<BagPartition> getPartitions(RemotingContext context, String instance, String bag) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).partitions().getConnectionString();
        ClientResponse clientResponse = doRequest(context, connectionString);

        return parseAsPartitionCollection(clientResponse);
    }

    @Override
    public BagPartition getPartition(RemotingContext context, String instance, String bag, String partitionId) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).partition(partitionId).getConnectionString();
        ClientResponse response = doRequest(context, connectionString);
        try {
            Partition partition = new JsonRestResponseHandler().deserialize(Partition.class, response.getEntityInputStream());
            return PartitionModelMapper.map(partition);
        } catch(RestException e) {
            throw new RemoteException("Unable to retrieve remote partition information", e);
        }
    }

    @Override
    public List<BagPartition> doPartitionOperation(RemotingContext context, String instance, String bag, String partitionId, String operation) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).partition(partitionId).doOperation(operation).getConnectionString();
        ClientResponse response = doRequest(context, connectionString);

        return parseAsPartitionCollection(response);
    }

    @Override
    public QueryResult getPartitionEntities(RemotingContext context, String instance, String bag, String partitionId, int start, int limit) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).partition(partitionId).entities().getConnectionString();
        Map<String, String> params = new HashMap<>();
        if(start > 0) params.put("begin", String.valueOf(start));
        if(limit > 0) params.put("top", String.valueOf(limit));
        ClientResponse clientResponse = doRequest(context, connectionString, params);

        try {
            StreamableEntityCollection entityCollection = new JsonRestResponseHandler().deserialize(StreamableEntityCollection.class, clientResponse.getEntityInputStream());
            return entityCollection.getResult();
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote entity data", e);
        }
    }

    private List<BagPartition> parseAsPartitionCollection(ClientResponse response) throws RemoteException {
        try {
            PartitionCollection partitionCollection = new JsonRestResponseHandler().deserialize(PartitionCollection.class, response.getEntityInputStream());
            List<Partition> partitions = partitionCollection.getPartitions();

            List<BagPartition> mappedPartitions = new ArrayList<>();
            for(Partition partition : partitions) {
                mappedPartitions.add(PartitionModelMapper.map(partition));
            }
            return mappedPartitions;
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote partition data", e);
        }
    }

}
