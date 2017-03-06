package com.oberasoftware.jasdb.service.local;

import com.oberasoftware.jasdb.service.JasDBMain;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public class LocalDBSessionFactory implements DBSessionFactory {
    private String instance;

    @Override
    public DBSession createSession() throws JasDBStorageException {
        if(StringUtils.stringNotEmpty(instance)) {
            return new LocalDBSession(instance);
        } else {
            return new LocalDBSession();
        }
    }

    @Override
    public DBSession createSession(Credentials credentials) throws JasDBStorageException {
        return new LocalDBSession(credentials);
    }

    @Override
    public DBSession createSession(String instance) throws JasDBStorageException {
        return new LocalDBSession(instance);
    }

    @Override
    public DBSession createSession(String instance, Credentials credentials) throws JasDBStorageException {
        if(credentials != null) {
            return new LocalDBSession(instance, credentials);
        } else {
            return new LocalDBSession(instance);
        }
    }

    public void shutdown() throws JasDBException {
        JasDBMain.shutdown();
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }
}
