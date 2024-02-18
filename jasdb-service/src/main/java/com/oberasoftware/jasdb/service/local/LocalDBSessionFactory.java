package com.oberasoftware.jasdb.service.local;

import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.api.session.DBSession;
import com.oberasoftware.jasdb.api.session.DBSessionFactory;
import com.oberasoftware.jasdb.api.security.Credentials;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import org.springframework.stereotype.Component;

/**
 * @author Renze de Vries
 */
@Component
public class LocalDBSessionFactory implements DBSessionFactory {
    private String instance;

    @Override
    public DBSession createSession() throws JasDBException {
        if(StringUtils.stringNotEmpty(instance)) {
            return new LocalDBSession(instance);
        } else {
            return new LocalDBSession();
        }
    }

    @Override
    public DBSession createSession(Credentials credentials) throws JasDBException {
        return new LocalDBSession(credentials);
    }

    @Override
    public DBSession createSession(String instance) throws JasDBException {
        return new LocalDBSession(instance);
    }

    @Override
    public DBSession createSession(String instance, Credentials credentials) throws JasDBException {
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
