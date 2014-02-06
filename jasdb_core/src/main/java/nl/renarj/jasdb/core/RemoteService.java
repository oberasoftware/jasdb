/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core;

import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.ServiceException;
import nl.renarj.jasdb.core.locator.ServiceInformation;

/**
 * This represents the remote service that can be started by the kernel for providing
 * the jasdb data services remotely
 *
 * @author Renze de Vries
 */
public interface RemoteService {
    /**
     * Returns wether this remote service is enabled
     * @return True is the service is enabled, False if not
     */
    boolean isEnabled();

    /**
     * Called during kernel startup, can be used to initialize any servicing components
     * @throws ServiceException If unable to start the remote service
     */
    void startService() throws JasDBException;

    /**
     * Called during kernel shutdown, can be used to cleanup used resources
     * @throws ServiceException If unable to cleanly stop the remote service
     */
    void stopService() throws JasDBException;

    /**
     * Provides servicing information that can be used by remote endpoints
     * to connect to this remote service. This can be used for auto discovery in grid mode.
     *
     * @return The service information
     */
    ServiceInformation getServiceInformation();
}
