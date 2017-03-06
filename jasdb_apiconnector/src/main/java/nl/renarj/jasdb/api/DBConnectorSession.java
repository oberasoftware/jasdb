/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api;

import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.BagConnector;
import nl.renarj.jasdb.remote.InstanceConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.remote.model.RemoteBag;
import com.oberasoftware.jasdb.engine.metadata.BagMeta;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public abstract class DBConnectorSession implements DBSession {
    private NodeInformation nodeInformation;
    private String instance;

    public DBConnectorSession(String instance, NodeInformation nodeInformation) throws JasDBStorageException {
        this.nodeInformation = nodeInformation;
        authenticate(null);
        validateInstance(instance);
    }

    public DBConnectorSession(String instance, Credentials credentials, NodeInformation nodeInformation) throws JasDBStorageException {
        this.nodeInformation = nodeInformation;
        authenticate(credentials);
        validateInstance(instance);
    }

    private void validateInstance(String instance) throws JasDBStorageException {
        try {
            InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
            if(instanceConnector.getInstance(getContext(), instance) != null) {
                this.instance = instance;
            } else {
                throw new JasDBStorageException("Instance with id: " + instance + " does not exist");
            }
        } catch(RemoteException e) {
            throw new JasDBStorageException("Unable to load instance", e);
        }
    }

    @Override
    public UserAdministration getUserAdministration() throws JasDBStorageException {
        return new RemoteUserAdministration(nodeInformation, getContext());
    }

    protected NodeInformation getNodeInformation() {
        return this.nodeInformation;
    }

    @Override
    public List<Instance> getInstances() throws JasDBStorageException {
        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
        return instanceConnector.getInstances(getContext());
    }

    @Override
    public Instance getInstance(String instanceId) throws JasDBStorageException {
        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);

        return instanceConnector.getInstance(getContext(), instanceId);
    }

    @Override
    public void addInstance(String instanceId) throws JasDBStorageException {
        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
        instanceConnector.addInstance(getContext(), instanceId);
    }

    @Override
    public void addAndSwitchInstance(String instanceId) throws JasDBStorageException {
        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
        instanceConnector.addInstance(getContext(), instanceId);

        instance = instanceId;
    }

    @Override
    public void switchInstance(String instanceId) throws JasDBStorageException {
        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
        if(instanceConnector.getInstance(getContext(), instanceId) != null) {
            this.instance = instanceId;
        } else {
            throw new JasDBStorageException("Unable to retrieve instance: " + instanceId);
        }
    }

    @Override
    public Instance deleteInstance(String instanceId) throws JasDBStorageException {
        if(this.instance.equals(instanceId)) {
            throw new JasDBStorageException("Cannot delete active instance over remote connection, switch to another instance of default instance");
        }

        InstanceConnector instanceConnector = RemoteConnectorFactory.createConnector(nodeInformation, InstanceConnector.class);
        instanceConnector.removeInstance(getContext(), instanceId);

        return instanceConnector.getInstance(getContext(), instance);

    }

    @Override
    public String getInstanceId() throws JasDBStorageException {
        doInstanceCheck();
        return instance;
    }

    private void doInstanceCheck() throws JasDBStorageException {
        if(instance == null) {
            throw new JasDBStorageException("DB current session has no active instance bound");
        }
    }

    @Override
    public EntityBag createOrGetBag(String bagName) throws JasDBStorageException {
        doInstanceCheck();
        return createOrGetBag(instance, bagName);
    }

    @Override
    public EntityBag createOrGetBag(String instanceId, String bagName) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        Bag meta = connector.getBag(getContext(), instanceId, bagName);
        if(meta == null) {
            meta = connector.createBag(getContext(), instanceId, new BagMeta(instanceId, bagName, new ArrayList<IndexDefinition>()));
        }

        return new RemoteEntityBag(instanceId, getContext(), nodeInformation, meta);
    }

    @Override
    public EntityBag getBag(String bagName) throws JasDBStorageException {
        doInstanceCheck();
        return getBag(instance, bagName);
    }

    @Override
    public EntityBag getBag(String instanceId, String bagName) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        Bag meta = connector.getBag(getContext(), instanceId, bagName);
        if(meta != null) {
            return new RemoteEntityBag(instanceId, getContext(), nodeInformation, meta);
        } else {
            return null;
        }
    }

    @Override
    public List<EntityBag> getBags() throws JasDBStorageException {
        doInstanceCheck();
        return getBags(instance);
    }

    @Override
    public List<EntityBag> getBags(String instanceId) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        List<RemoteBag> bagMetas = connector.getBags(getContext(), instanceId);
        List<EntityBag> remoteConnectedEntityBags = new LinkedList<>();
        for(RemoteBag bagMeta : bagMetas) {
            remoteConnectedEntityBags.add(new RemoteEntityBag(instanceId, getContext(), nodeInformation, bagMeta));
        }

        return remoteConnectedEntityBags;
    }

    @Override
    public void removeBag(String bagName) throws JasDBStorageException {
        doInstanceCheck();
        removeBag(instance, bagName);
    }

    @Override
    public void removeBag(String instanceId, String bagName) throws JasDBStorageException {
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        connector.removeBag(getContext(), instanceId, bagName);
    }

    protected abstract RemotingContext getContext();

    protected abstract void authenticate(Credentials credentials) throws JasDBStorageException;

    @Override
    public void closeSession() throws JasDBStorageException {

    }
}
