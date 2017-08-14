package com.oberasoftware.jasdb.integration;

import com.oberasoftware.jasdb.acl.BasicCredentials;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.service.local.LocalDBSessionFactory;

/**
* @author Renze de Vries
*/
public class OverrideSecureSessionFactory extends LocalDBSessionFactory {
    @Override
    public DBSession createSession(String instance) throws JasDBException {
        return super.createSession(instance, new BasicCredentials("admin", "localhost", ""));
    }

    @Override
    public DBSession createSession() throws JasDBException {
        return super.createSession(new BasicCredentials("admin", "localhost", ""));
    }
}
