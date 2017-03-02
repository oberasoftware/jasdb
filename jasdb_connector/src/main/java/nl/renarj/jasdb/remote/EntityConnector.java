package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.storage.query.operators.BlockOperation;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface EntityConnector extends  RemoteConnector {
    /* All entity modification operations */
    SimpleEntity insertEntity(RemotingContext context, String instance, String bag, SimpleEntity entity) throws RemoteException;

    SimpleEntity updateEntity(RemotingContext context, String instance, String bag, SimpleEntity entity) throws RemoteException;

    boolean removeEntity(RemotingContext context, String instance, String bag, String entityId) throws RemoteException;

    /* All entity retrieval operations */
    QueryResult find(RemotingContext context, String instance, String bag, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws RemoteException;

    QueryResult find(RemotingContext context, String instance, String bag) throws RemoteException;

    QueryResult find(RemotingContext context, String instance, String bag, int max) throws RemoteException;

    SimpleEntity findById(RemotingContext context, String instance, String bag, String id) throws RemoteException;
}
