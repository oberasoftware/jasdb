/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.locator;

import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * User: renarj
 * Date: 1/18/12
 * Time: 10:24 PM
 */
public class GridLocatorUtil {
    private static final Logger logger = LoggerFactory.getLogger(GridLocatorUtil.class);
    public static List<InetAddress> getAdresses() throws ConfigurationException {
        List<InetAddress> networkInetAdresses = new ArrayList<InetAddress>();
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumerator = NetworkInterface.getNetworkInterfaces();
            while(networkInterfaceEnumerator.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaceEnumerator.nextElement();
                Enumeration<InetAddress> addressEnumeration = networkInterface.getInetAddresses();
                while(addressEnumeration.hasMoreElements()) {
                    InetAddress address = addressEnumeration.nextElement();
                    if(!address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                        networkInetAdresses.add(address);
                    }
                }
            }
        } catch(SocketException e) {
            throw new ConfigurationException("Unable to get network interfaces to check for grid information", e);
        }
        return networkInetAdresses;
    }
    
    public static InetAddress getPublicAddress() throws ConfigurationException {
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while(networkInterfaceEnumeration.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaceEnumeration.nextElement();
                Enumeration<InetAddress> addressEnumeration = networkInterface.getInetAddresses();
                while(addressEnumeration.hasMoreElements()) {
                    InetAddress address = addressEnumeration.nextElement();
                    if(!address.isLinkLocalAddress() && !address.isLoopbackAddress()) {
                        logger.debug("Found remote available network interface address: {}", address);
                        return address;
                    }
                }
            }
        } catch(SocketException e) {
            logger.error("Unable to load network interface information", e);
        }

        throw new ConfigurationException("Unable to get network interfaces to check for grid information"); 
    }

    public static String getServiceType(String gridId) {
        String g = gridId;
        if(gridId != null) {
            g = gridId.replace(".", "_");
        } else {
            g = "";
        }

        return "_" + gridId + "jasdb._tcp.local.";
    }

    public static String getServiceType(NodeInformation nodeInformation) {
        return getServiceType(nodeInformation.getGridId());
    }

}
