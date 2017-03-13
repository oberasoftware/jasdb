package nl.renarj.jasdb.remote;

import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.remote.model.RemoteBag;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface BagConnector extends RemoteConnector {
    /* All bag related operations */
    RemoteBag getBag(RemotingContext context, String instance, String bag) throws RemoteException;

    List<RemoteBag> getBags(RemotingContext context, String instance) throws RemoteException;

    Bag createBag(RemotingContext context, String instance, Bag bag) throws RemoteException;

    boolean removeBag(RemotingContext context, String instance, String bagName) throws RemoteException;

    boolean flushBag(RemotingContext context, String instance, String bagName) throws RemoteException;

    /* All index operations */
    List<IndexDefinition> getIndexDefinitions(RemotingContext context, String instance, String bag) throws RemoteException;

    IndexDefinition createIndex(RemotingContext context, String instance, String bag, IndexDefinition definition, boolean isUnique) throws RemoteException;

    boolean removeIndex(RemotingContext context, String instance, String bag, String indexName) throws RemoteException;
}
