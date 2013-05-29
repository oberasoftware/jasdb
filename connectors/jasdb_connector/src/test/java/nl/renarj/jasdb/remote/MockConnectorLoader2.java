/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

/**
 * User: renarj
 * Date: 1/26/12
 * Time: 9:32 PM
 */
public class MockConnectorLoader2 implements ConnectorLoader {
    @Override
    public String getConnectorType() {
        return "mock2";
    }

    @Override
    public <T extends RemoteConnector> T createConnector(NodeInformation nodeInformation, Class<T> operationType) throws RemoteException {
        return operationType.cast(new MockConnector2());
    }

    @Override
    public String getCachingKey(NodeInformation nodeInformation, Class<? extends RemoteConnector> operationType) {
        return null;
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
