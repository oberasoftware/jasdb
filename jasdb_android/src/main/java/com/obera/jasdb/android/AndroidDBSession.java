package com.obera.jasdb.android;

import android.content.Context;
import com.obera.jasdb.android.api.AndroidEntityBag;
import com.obera.jasdb.android.platform.AndroidContext;
import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import com.oberasoftware.jasdb.entitymapper.EntityManagerImpl;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.UserAdministration;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class AndroidDBSession implements DBSession {
    private DBInstance instance;
    private DBInstanceFactory instanceFactory;

    public AndroidDBSession(Context context) throws JasDBStorageException {
        initializeSystemContext(context);

        instanceFactory = SimpleKernel.getInstanceFactory();
        instance = instanceFactory.getInstance();
    }

    public AndroidDBSession(Context context, String instanceId) throws JasDBStorageException {
        initializeSystemContext(context);

        instanceFactory = SimpleKernel.getInstanceFactory();
        instance = instanceFactory.getInstance(instanceId);
    }

    private void initializeSystemContext(Context context) {
//        PlatformManagerFactory.setPlatformManager(new AndroidPlatformManager());
        AndroidContext.setContext(context);
    }

    @Override
    public UserAdministration getUserAdministration() throws JasDBStorageException {
        throw new JasDBSecurityException("Not supported on Android");
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
    public EntityManager getEntityManager() {
        return new EntityManagerImpl(this);
    }

    @Override
    public String getInstanceId() throws JasDBStorageException {
        return instance != null ? instance.getInstanceId() : null;
    }

    @Override
    public EntityBag createOrGetBag(String bagName) throws JasDBStorageException {
        return new AndroidEntityBag(instance.getInstanceId(), bagName);
    }

    @Override
    public EntityBag createOrGetBag(String instanceId, String bagName) throws JasDBStorageException {
        return new AndroidEntityBag(instanceId, bagName);
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
            return new AndroidEntityBag(bagInstance.getInstanceId(), bagMeta.getName());
        } else {
            return null;
        }
    }

    @Override
    public DBInstance getInstance(String instanceId) throws JasDBStorageException {
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
        List<EntityBag> bags = new ArrayList<>();
        for(Bag bagMeta : bagMetas) {
            bags.add(new AndroidEntityBag(bagInstance.getInstanceId(), bagMeta.getName()));
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
