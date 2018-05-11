package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.model.ServiceInformation;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface RemoteServiceManager {
    void startRemoteServices() throws JasDBException;

    void stopRemoteServices() throws JasDBException;

    List<ServiceInformation> getServiceInformation();
}
