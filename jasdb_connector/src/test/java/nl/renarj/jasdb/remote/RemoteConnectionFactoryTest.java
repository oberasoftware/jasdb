/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: renarj
 * Date: 1/26/12
 * Time: 9:29 PM
 */
public class RemoteConnectionFactoryTest {
    @Test
    public void testConnectorLoadingMock1() throws RemoteException {
        Map<String, String> params = new HashMap<>();
        params.put(RemoteConnectorFactory.CONNECTOR_TYPE, "mock1");
        NodeInformation nodeInformation = new NodeInformation("localhost", "instance1");
        nodeInformation.addServiceInformation(new ServiceInformation("mock1", params));
        RemoteConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, MockConnector1.class);

        Assert.assertTrue(connector instanceof MockConnector1);
    }

    @Test
    public void testConnectorLoadingMock2() throws RemoteException {
        Map<String, String> params = new HashMap<>();
        params.put(RemoteConnectorFactory.CONNECTOR_TYPE, "mock2");
        NodeInformation nodeInformation = new NodeInformation("localhost", "instance1");
        nodeInformation.addServiceInformation(new ServiceInformation("mock2", params));
        RemoteConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, MockConnector2.class);
        Assert.assertTrue(connector instanceof MockConnector2);
    }
}
