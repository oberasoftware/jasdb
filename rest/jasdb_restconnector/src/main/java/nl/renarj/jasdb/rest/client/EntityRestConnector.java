package nl.renarj.jasdb.rest.client;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.remote.EntityConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.model.streaming.StreamedEntity;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Renze de Vries
 */
public class EntityRestConnector extends RemoteRestConnector implements EntityConnector {
    public EntityRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public SimpleEntity insertEntity(RemotingContext context, String instance, String bag, SimpleEntity entity) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities().getConnectionString();
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(new StreamedEntity(entity), bos);

            ClientResponse clientResponse = doInternalRequest(context, connectionString, new HashMap<String, String>(), bos.toByteArray(), REQUEST_MODE.POST);

            try {
                StreamedEntity returnedEntity = new JsonRestResponseHandler().deserialize(StreamedEntity.class, clientResponse.getEntityInputStream());
                entity.setInternalId(returnedEntity.getEntity().getInternalId());
                return returnedEntity.getEntity();
            } finally {
                clientResponse.close();
            }
        } catch(RestException e) {
            throw new RemoteException("Unable to insert entity on remote destination", e);
        }
    }

    @Override
    public SimpleEntity updateEntity(RemotingContext context, String instance, String bag, SimpleEntity entity) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities().getConnectionString();
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(new StreamedEntity(entity), bos);

            ClientResponse clientResponse = doRequest(context, connectionString, new HashMap<String, String>(), bos.toString(CHARACTER_ENCODING), REQUEST_MODE.PUT);
            try {
            	StreamedEntity returnedEntity = new JsonRestResponseHandler().deserialize(StreamedEntity.class, clientResponse.getEntityInputStream());
            	return returnedEntity.getEntity();
            } finally {
            	clientResponse.close();
            }            
        } catch(RestException e) {
            throw new RemoteException("Unable to update entity on remote destination", e);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("Unable to serialize entity", e);
        }
    }

    @Override
    public boolean removeEntity(RemotingContext context, String instance, String bag, String entityId) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities(entityId).getConnectionString();

        doRequest(context, connectionString, new HashMap<String, String>(), null, REQUEST_MODE.DELETE).close();
        return true;
    }

    @Override
    public QueryResult find(RemotingContext context, String instance, String bag, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> sortParams) throws RemoteException {
        String query = RestQueryGenerator.generatorQuery(blockOperation);
        String orderParams = RestQueryGenerator.generateOrderParams(sortParams);
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities(query).getConnectionString();

        Map<String, String> params = new HashMap<String, String>();
        if(limit.getBegin() > 0) {
            params.put("begin", String.valueOf(limit.getBegin()));
        }
        if(limit.getMax() > 0) {
            params.put("top", String.valueOf(limit.getMax()));
        }
        if(StringUtils.stringNotEmpty(orderParams)) params.put("orderBy", orderParams);
        ClientResponse response = doRequest(context, connectionString, params);

        return parseAsEntityCollection(response);
    }

    @Override
    public QueryResult find(RemotingContext context, String instance, String bag) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities().getConnectionString();
        ClientResponse response = doRequest(context, connectionString);

        return parseAsEntityCollection(response);
    }

    @Override
    public QueryResult find(RemotingContext context, String instance, String bag, int limit) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entities().getConnectionString();
        Map<String, String> params = new HashMap<String, String>();
        if(limit > 0) {
            params.put("top", "" + limit);
        }

        ClientResponse response = doRequest(context, connectionString, params);

        return parseAsEntityCollection(response);
    }

    @Override
    public SimpleEntity findById(RemotingContext context, String instance, String bag, String id) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instance).bag(bag).entityById(id).getConnectionString();
        ClientResponse response = doRequest(context, connectionString);
        try {
            StreamedEntity entity = new JsonRestResponseHandler().deserialize(StreamedEntity.class, response.getEntityInputStream());
            return entity.getEntity();
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote entity data", e);
        } finally {
        	response.close();
        }
    }

    private QueryResult parseAsEntityCollection(ClientResponse response) throws RemoteException {
        try {
            StreamableEntityCollection entityCollection =  new JsonRestResponseHandler().deserialize(StreamableEntityCollection.class, new WrappedInputStream(response));
            return entityCollection.getResult();
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote entity data", e);
        }
    }

    private class WrappedInputStream extends InputStream {
        private ClientResponse clientResponse;
        private InputStream inputStream;

        private WrappedInputStream(ClientResponse clientResponse) {
            this.clientResponse = clientResponse;
            this.inputStream = clientResponse.getEntityInputStream();
        }

        @Override
        public int read() throws IOException {
            return inputStream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return inputStream.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return inputStream.skip(n);
        }

        @Override
        public int available() throws IOException {
            return inputStream.available();
        }

        @Override
        public void close() throws IOException {
            clientResponse.close();
        }

        @Override
        public boolean markSupported() {
            return inputStream.markSupported();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return inputStream.read(b);
        }

        @Override
        public synchronized void mark(int readlimit) {
            inputStream.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            inputStream.reset();
        }
    }

}
