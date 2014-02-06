package com.obera.jasdb.local;

import com.obera.service.acl.BasicCredentials;
import nl.renarj.jasdb.LocalDBSessionFactory;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
* @author Renze de Vries
*/
public class OverrideSecureSessionFactory extends LocalDBSessionFactory {
    @Override
    public DBSession createSession(String instance) throws JasDBStorageException {
        return super.createSession(instance, new BasicCredentials("admin", "localhost", ""));
    }

    @Override
    public DBSession createSession() throws JasDBStorageException {
        return super.createSession(new BasicCredentials("admin", "localhost", ""));
    }
}
