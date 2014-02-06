package com.obera.jasdb.android;

import nl.renarj.jasdb.core.RemoteService;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.ServiceInformation;

/**
 * @author renarj
 */
public class DummyRemoteService implements RemoteService {
    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void startService() throws JasDBException {

    }

    @Override
    public void stopService() throws JasDBException {

    }

    @Override
    public ServiceInformation getServiceInformation() {
        return null;
    }
}
