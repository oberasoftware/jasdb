/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.UserAdministration;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.storage.entities.EntityBagImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * This creates a local DB session, JasDB will run inside the same process.
 *
 * @author Renze de Vries
 */
public class LocalDBSession implements DBSession {
    private DBInstance instance;
	private DBInstanceFactory instanceFactory;

    private UserSession userSession;

    /**
     * Creates a local DB session to the default instance
     * @throws JasDBStorageException If unable to request the session
     */
	public LocalDBSession() throws JasDBStorageException {
        instanceFactory = SimpleKernel.getInstanceFactory();
        instance = instanceFactory.getInstance();
	}

    /**
     * Creates a local DB session with credentials
     * @param credentials The credentials
     * @throws JasDBStorageException If unable to request the session
     */
    public LocalDBSession(Credentials credentials) throws JasDBStorageException {
        this();

        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
        userSession = sessionManager.startSession(credentials);
    }

    /**
     * Creates a local DB session bound to a specific instance
     * @param instanceId The instance
     * @throws JasDBStorageException If unable to request the session
     */
	public LocalDBSession(String instanceId) throws JasDBStorageException {
		instance = SimpleKernel.getInstanceFactory().getInstance(instanceId);
	}

    /**
     * Creates a local DB session bound to a specific instance with given credentials
     * @param instanceId The instance
     * @param credentials The credentials
     * @throws JasDBStorageException If unable to request the session
     */
    public LocalDBSession(String instanceId, Credentials credentials) throws JasDBStorageException {
        this(instanceId);

        SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
        userSession = sessionManager.startSession(credentials);
    }

    @Override
    public UserAdministration getUserAdministration() throws JasDBStorageException {
        if(userSession != null) {
            return new LocalUserAdministration(userSession);
        } else {
            throw new JasDBSecurityException("Unable to get user administration, not logged in");
        }
    }

    @Override
    public List<Instance> getInstances() throws JasDBStorageException {
        return new ArrayList<Instance>(instanceFactory.listInstances());
    }

    @Override
    public void addInstance(String instanceId, String path) throws JasDBStorageException {
        instanceFactory.addInstance(instanceId, path);
    }

    @Override
    public void addAndSwitchInstance(String instanceId, String path) throws JasDBStorageException {
        addInstance(instanceId, path);
        switchInstance(instanceId);
    }

    @Override
    public void switchInstance(String instanceId) throws JasDBStorageException {
        instance = getInstance(instanceId);
    }

    @Override
    public Instance deleteInstance(String instanceId) throws JasDBStorageException {
        Instance deleteInstance = getInstance(instanceId);
        //in case we are deleting the current connected instance let's switch
        if(this.instance != null && deleteInstance.getInstanceId().equals(this.instance.getInstanceId())) {
            this.instance = instanceFactory.getInstance();
        }

        instanceFactory.deleteInstance(instanceId);

        return this.instance;
    }

    @Override
    public String getInstanceId() throws JasDBStorageException {
        return instance != null ? instance.getInstanceId() : null;
    }

    @Override
	public EntityBag createOrGetBag(String bagName) throws JasDBStorageException {
		return new EntityBagImpl(instance.getInstanceId(), bagName, userSession);
	}

    @Override
    public EntityBag createOrGetBag(String instanceId, String bagName) throws JasDBStorageException {
        return new EntityBagImpl(getInstance(instanceId).getInstanceId(), bagName, userSession);
    }

    @Override
    public EntityBag getBag(String bagName) throws JasDBStorageException {
        return getBag(instance, bagName);
    }

    @Override
    public EntityBag getBag(String instanceId, String bagName) throws JasDBStorageException {
        DBInstance bagInstance = getInstance(instanceId);
        return getBag(bagInstance, bagName);
    }

    private EntityBag getBag(DBInstance bagInstance, String bagName) throws JasDBStorageException {
        Bag bagMeta = bagInstance.getBag(bagName);
        if(bagMeta != null) {
            return new EntityBagImpl(bagInstance.getInstanceId(), bagMeta.getName(), userSession);
        } else {
            return null;
        }
    }

    private DBInstance getInstance(String instanceId) throws JasDBStorageException {
        try {
            return instanceFactory.getInstance(instanceId);
        } catch(ConfigurationException e) {
            throw new JasDBStorageException("Unable to retrieve instance: " + instanceId, e);
        }
    }

    @Override
    public List<EntityBag> getBags() throws JasDBStorageException {
        return getBags(instance);
	}

    @Override
    public List<EntityBag> getBags(String instanceId) throws JasDBStorageException {
        return getBags(getInstance(instanceId));
    }

    private List<EntityBag> getBags(DBInstance bagInstance) throws JasDBStorageException {
        List<Bag> bagMetas = bagInstance.getBags();
        List<EntityBag> bags = new ArrayList<EntityBag>();
        for(Bag bagMeta : bagMetas) {
            bags.add(new EntityBagImpl(bagInstance.getInstanceId(), bagMeta.getName(), userSession));
        }

        return bags;
    }

    @Override
    public void removeBag(String bagName) throws JasDBStorageException {
        instance.removeBag(bagName);
    }

    @Override
    public void removeBag(String instanceId, String bagName) throws JasDBStorageException {
        getInstance(instanceId).removeBag(bagName);
    }

    @Override
    public void closeSession() throws JasDBStorageException {
		
	}
}
