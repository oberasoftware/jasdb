package nl.renarj.jasdb.api.engine;

import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.ServiceInformation;

import java.util.List;

/**
 * @author renarj
 */
public interface RemoteServiceManager {
    void startRemoteServices() throws JasDBException;

    void stopRemoteServices() throws JasDBException;

    List<ServiceInformation> getServiceInformation();
}
