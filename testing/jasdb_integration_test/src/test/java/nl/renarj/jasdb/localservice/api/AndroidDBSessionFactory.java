package nl.renarj.jasdb.localservice.api;

import com.obera.jasdb.android.AndroidDBSession;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public class AndroidDBSessionFactory implements DBSessionFactory {
    private String instance;

    @Override
    public DBSession createSession() throws JasDBStorageException {
        if(StringUtils.stringNotEmpty(instance)) {
            return new AndroidDBSession(null, instance);
        } else {
            return new AndroidDBSession(null);
        }
    }

    @Override
    public DBSession createSession(Credentials credentials) throws JasDBStorageException {
        return new AndroidDBSession(null);
    }

    @Override
    public DBSession createSession(String instance) throws JasDBStorageException {
        return new AndroidDBSession(null, instance);
    }

    @Override
    public DBSession createSession(String instance, Credentials credentials) throws JasDBStorageException {
        return new AndroidDBSession(null, instance);
    }

    public void shutdown() throws JasDBException {
        SimpleKernel.shutdown();
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }
}
