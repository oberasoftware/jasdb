package com.oberasoftware.jasdb.rest.client;

import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import nl.renarj.jasdb.remote.BagConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.remote.model.RemoteBag;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.mappers.IndexModelMapper;
import com.oberasoftware.jasdb.rest.model.BagCollection;
import com.oberasoftware.jasdb.rest.model.IndexCollection;
import com.oberasoftware.jasdb.rest.model.IndexEntry;
import com.oberasoftware.jasdb.rest.model.RestBag;
import com.oberasoftware.jasdb.rest.model.serializers.json.JsonRestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class BagRestConnector extends RemoteRestConnector implements BagConnector {
    private static final Logger log = LoggerFactory.getLogger(BagRestConnector.class);

    public BagRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public RemoteBag getBag(RemotingContext context, String instance, String bagName) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bagName).getConnectionString();
        try {
            ClientResponse clientResponse = doRequest(context, connectionString);
            try {
                RestBag bag = new JsonRestResponseHandler().deserialize(RestBag.class, clientResponse.getEntityInputStream());
                return new RemoteBag(bag.getInstanceId(), bag.getName(), new ArrayList<>(), bag.getSize(), bag.getDiskSize());
            } catch(RestException e) {
                throw new RemoteException("Unable to parse remote bag data", e);
            } finally {
            	clientResponse.close();
            }
        } catch(ResourceNotFoundException e) {
            log.debug("No resource was found for bag: {} on instance: {}", bagName, instance);
            return null;
        }
    }

    @Override
    public List<RemoteBag> getBags(RemotingContext context, String instance) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bags().getConnectionString();
        ClientResponse clientResponse = doRequest(context, connectionString);

        try {
            BagCollection bagCollection = new JsonRestResponseHandler().deserialize(BagCollection.class, clientResponse.getEntityInputStream());
            List<RemoteBag> mappedBags = new ArrayList<>();
            for(RestBag bag : bagCollection.getBags()) {
                mappedBags.add(new RemoteBag(bag.getInstanceId(), bag.getName(), new ArrayList<>(), bag.getSize(), bag.getDiskSize()));
            }

            return mappedBags;
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote bag data", e);
        } finally {
        	clientResponse.close();
        }
    }

    @Override
    public RemoteBag createBag(RemotingContext context, String instance, Bag bagMeta) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bags().getConnectionString();
        try {
            RestBag bag = new RestBag(instance, bagMeta.getName(), 0, 0);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(bag, bos);

            ClientResponse clientResponse = doRequest(context, connectionString, new HashMap<>(), bos.toString(CHARACTER_ENCODING), REQUEST_MODE.POST);
            try {
            	bag = new JsonRestResponseHandler().deserialize(RestBag.class, clientResponse.getEntityInputStream());
            } finally {
            	clientResponse.close();
            }
            return new RemoteBag(bag.getInstanceId(), bag.getName(), new ArrayList<>(), bag.getSize(), bag.getDiskSize());
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote bag definition", e);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("Unable to serialize bag", e);
        }
    }

    @Override
    public boolean removeBag(RemotingContext context, String instance, String bagName) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bagName).getConnectionString();
        doRequest(context, connectionString, new HashMap<>(), null, REQUEST_MODE.DELETE).close();

        return true;
    }

    @Override
    public boolean flushBag(RemotingContext context, String instance, String bagName) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bagName).doOperation("flush").getConnectionString();
        doRequest(context, connectionString);

        return true;
    }

    @Override
    public boolean removeIndex(RemotingContext context, String instance, String bag, String indexName) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).index(indexName).getConnectionString();

        doRequest(context, connectionString, new HashMap<>(), null, REQUEST_MODE.DELETE).close();
        return true;
    }

    @Override
    public List<IndexDefinition> getIndexDefinitions(RemotingContext context, String instance, String bag) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).indexes().getConnectionString();
        ClientResponse clientResponse = doRequest(context, connectionString);

        try {
            IndexCollection indexCollection = new JsonRestResponseHandler().deserialize(IndexCollection.class, clientResponse.getEntityInputStream());
            List<IndexDefinition> indexDefinitions = new ArrayList<>();
            for(IndexEntry entry : indexCollection.getIndexEntryList()) {
                indexDefinitions.add(IndexModelMapper.map(entry));
            }
            return indexDefinitions;
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote index definitions", e);
        } finally {
        	clientResponse.close();
        }
    }

    @Override
    public IndexDefinition createIndex(RemotingContext context, String instance, String bag, IndexDefinition definition, boolean isUnique) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).indexes().getConnectionString();
        try {
            IndexEntry entry = IndexModelMapper.map(definition, isUnique);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(entry, bos);

            ClientResponse clientResponse = doRequest(context, connectionString, new HashMap<>(), bos.toString(CHARACTER_ENCODING), REQUEST_MODE.POST);
            try {
            	entry = new JsonRestResponseHandler().deserialize(IndexEntry.class, clientResponse.getEntityInputStream());
            } finally {
            	clientResponse.close();
            }

            return IndexModelMapper.map(entry);
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote index definition", e);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("Unable to serialize entity", e);
        }
    }

}
