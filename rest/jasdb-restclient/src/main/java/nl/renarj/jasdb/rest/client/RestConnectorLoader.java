/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.*;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.Map;

/**
 * User: renarj
 * Date: 1/26/12
 * Time: 8:52 PM
 */
public class RestConnectorLoader implements ConnectorLoader {
    private static final String CONNECTOR_TYPE = "rest";

    @Override
    public String getConnectorType() {
        return CONNECTOR_TYPE;
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public String getCachingKey(NodeInformation nodeInformation, Class<? extends RemoteConnector> operationInterface) {
        Map<String, String> props = nodeInformation.getServiceInformation("rest").getNodeProperties();
        return nodeInformation.getInstanceId() + props.get(RemoteRestConnector.CONNECTION_HOST_PROPERTY) + props.get(RemoteRestConnector.CONNECTION_PORT_PROPERTY) + operationInterface.getName();
    }

    @Override
    public <T extends RemoteConnector> T createConnector(NodeInformation nodeInformation, Class<T> operationInterface) throws RemoteException {
        try {
            if(operationInterface.equals(EntityConnector.class)) {
                return operationInterface.cast(new EntityRestConnector(nodeInformation));
            } else if(operationInterface.equals(InstanceConnector.class)) {
                return operationInterface.cast(new InstanceRestConnector(nodeInformation));
            } else if(operationInterface.equals(BagConnector.class)) {
                return operationInterface.cast(new BagRestConnector(nodeInformation));
            } else if(operationInterface.equals(TokenConnector.class)) {
                return operationInterface.cast(new OAuthConnector(nodeInformation));
            } else if(operationInterface.equals(UserConnector.class)) {
                return operationInterface.cast(new UserRestConnector(nodeInformation));
            } else {
                throw new RemoteException("Unable to create connector, no support for operations of type: " + operationInterface.getName());
            }
        } catch(ConfigurationException e) {
            throw new RemoteException("Unable to load remote connector for rest service", e);
        }
    }

	@Override
	public void shutdown() {
		
	}
}
