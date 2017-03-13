package nl.renarj.jasdb.remote;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.api.session.query.SortParameter;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface EntityConnector extends  RemoteConnector {
    /* All entity modification operations */
    Entity insertEntity(RemotingContext context, String instance, String bag, Entity entity) throws RemoteException;

    Entity updateEntity(RemotingContext context, String instance, String bag, Entity entity) throws RemoteException;

    boolean removeEntity(RemotingContext context, String instance, String bag, String entityId) throws RemoteException;

    /* All entity retrieval operations */
    QueryResult find(RemotingContext context, String instance, String bag, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws RemoteException;

    QueryResult find(RemotingContext context, String instance, String bag) throws RemoteException;

    QueryResult find(RemotingContext context, String instance, String bag, int max) throws RemoteException;

    Entity findById(RemotingContext context, String instance, String bag, String id) throws RemoteException;
}
