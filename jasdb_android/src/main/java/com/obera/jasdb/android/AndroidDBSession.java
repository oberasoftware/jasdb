package com.obera.jasdb.android;

import android.content.Context;
import com.obera.jasdb.android.platform.AndroidContext;
import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.UserAdministration;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class AndroidDBSession implements DBSession {
    private LocalDBSession localDBSession;

    public AndroidDBSession(Context context) throws JasDBStorageException {
        initializeSystemContext(context);
        localDBSession = new LocalDBSession();
    }

    public AndroidDBSession(Context context, Credentials credentials) throws JasDBStorageException {
        initializeSystemContext(context);
        localDBSession = new LocalDBSession(credentials);
    }

    private void initializeSystemContext(Context context) {
        AndroidContext.setContext(context);
    }

    @Override
    public UserAdministration getUserAdministration() throws JasDBStorageException {
        return localDBSession.getUserAdministration();
    }

    @Override
    public List<Instance> getInstances() throws JasDBStorageException {
        return localDBSession.getInstances();
    }

    @Override
    public void addInstance(String instanceId, String path) throws JasDBStorageException {
        localDBSession.addInstance(instanceId, path);
    }

    @Override
    public void addAndSwitchInstance(String instanceId, String path) throws JasDBStorageException {
        localDBSession.addAndSwitchInstance(instanceId, path);
    }

    @Override
    public Instance deleteInstance(String instanceId) throws JasDBStorageException {
        return localDBSession.deleteInstance(instanceId);
    }

    @Override
    public void switchInstance(String instanceId) throws JasDBStorageException {
        localDBSession.switchInstance(instanceId);
    }

    @Override
    public String getInstanceId() throws JasDBStorageException {
        return localDBSession.getInstanceId();
    }

    @Override
    public EntityBag createOrGetBag(String bagName) throws JasDBStorageException {
        return localDBSession.createOrGetBag(bagName);
    }

    @Override
    public EntityBag createOrGetBag(String instanceId, String bagName) throws JasDBStorageException {
        return localDBSession.createOrGetBag(instanceId, bagName);
    }

    @Override
    public EntityBag getBag(String bagName) throws JasDBStorageException {
        return localDBSession.getBag(bagName);
    }

    @Override
    public EntityBag getBag(String instanceId, String bagName) throws JasDBStorageException {
        return localDBSession.getBag(instanceId, bagName);
    }

    @Override
    public List<EntityBag> getBags() throws JasDBStorageException {
        return localDBSession.getBags();
    }

    @Override
    public List<EntityBag> getBags(String instanceId) throws JasDBStorageException {
        return localDBSession.getBags(instanceId);
    }

    @Override
    public void removeBag(String bagName) throws JasDBStorageException {
        localDBSession.removeBag(bagName);
    }

    @Override
    public void removeBag(String instanceId, String bagName) throws JasDBStorageException {
        localDBSession.removeBag(instanceId, bagName);
    }

    @Override
    public void closeSession() throws JasDBStorageException {
        localDBSession.closeSession();
    }
}
