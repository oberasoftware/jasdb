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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
public class RemoteConnectorFactory {
    public static final String CONNECTOR_TYPE = "connectorType";
    private static RemoteConnectorFactory instance = new RemoteConnectorFactory();
    
    private Map<String, ConnectorLoader> loaderMap = new HashMap<String, ConnectorLoader>();

    private Map<String, RemoteConnector> cachedConnectorMap = new ConcurrentHashMap<String, RemoteConnector>();

    private String preferredType;
    
    private RemoteConnectorFactory() {
        ServiceLoader<ConnectorLoader> serviceLoader = ServiceLoader.load(ConnectorLoader.class);
        int highest = 0;
        for(ConnectorLoader loader : serviceLoader) {
            if(loader.priority() > highest) {
                highest = loader.priority();
                preferredType = loader.getConnectorType();
            }
            loaderMap.put(loader.getConnectorType(), loader);
        }
    }
    
    private <T extends RemoteConnector> T loadConnector(NodeInformation nodeInformation, String connectorType, Class<T> operationInterface) throws RemoteException {
        if(loaderMap.containsKey(connectorType)) {
            ConnectorLoader loader = loaderMap.get(connectorType);

            String cachingKey = loader.getCachingKey(nodeInformation, operationInterface);
            if(loader.isReusable() && cachedConnectorMap.containsKey(cachingKey)) {
                return operationInterface.cast(cachedConnectorMap.get(cachingKey));
            } else {
                T connector = loader.createConnector(nodeInformation, operationInterface);
                if(loader.isReusable()) {
                    cachedConnectorMap.put(cachingKey, connector);
                }
                return connector;
            }
        } else {
            throw new RemoteException("No known connectors found of type: " + connectorType);
        }
    }

    public static <T extends RemoteConnector> T createConnector(NodeInformation nodeInformation, Class<T> operationInterface) throws RemoteException {
        if(nodeInformation.getServiceInformationList().size() > 0) {
            String serviceType;
            if(nodeInformation.getServiceInformation(instance.preferredType) != null) {
                serviceType = instance.preferredType;
            } else {
                serviceType = nodeInformation.getServiceInformationList().get(0).getServiceType();
            }

            Map<String, ?> properties = nodeInformation.getServiceInformation(serviceType).getNodeProperties();

            if(serviceType != null && properties != null) {
                return instance.loadConnector(nodeInformation, serviceType, operationInterface);
            }
        }

        throw new RemoteException("Remote grid nodeInformation connection information contains not enough connection information: " + nodeInformation.toString());
    }
    
    public static void shutdown() {
    	for(ConnectorLoader connectorLoader : instance.loaderMap.values()) {
    		connectorLoader.shutdown();
    	}
    	
    	for(RemoteConnector cachedConnector : instance.cachedConnectorMap.values()) {
    		cachedConnector.close();
    	}
    }
}
