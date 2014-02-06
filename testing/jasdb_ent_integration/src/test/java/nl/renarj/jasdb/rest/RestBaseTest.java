/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest;

import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.locator.ServiceInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * User: renarj
 * Date: 1/26/12
 * Time: 7:34 PM
 */
public class RestBaseTest {
    protected static final String[] cities = new String[] {"Amsterdam", "Rotterdam", "NewYork", "Sydney", "Zurich", "SanFrancisco", "Rome", "Berlin"};

    private static final Logger logger = LoggerFactory.getLogger(RestBaseTest.class);

    public static String INSTANCE_ID = "default";
    public static String BAG_NAME = "bag0";
    public static int DEFAULT_PORT = 7050;
    public static int DEFAULT_SSL_PORT = 7051;
    
    @BeforeClass
    public static void startCleanEnvironment() {
        SimpleBaseTest.cleanData();
    }
    
    @Before
    public void setUp() throws Exception {
        logger.info("Starting Kernel");
        SimpleKernel.initializeKernel();
        logger.info("Finished starting kernel");
    }
    
    @After
    public void tearDown() throws Exception {
        logger.info("Stopping kernel");
        SimpleKernel.shutdown();
        logger.info("Kernel was stopped");

        SimpleBaseTest.cleanData();
    }
    
    protected NodeInformation constructNode(int portNr) {
        Map<String, String> gridProperties = new HashMap<>();
        gridProperties.put("connectorType", "rest");
        gridProperties.put("protocol", "http");
        gridProperties.put("host", "localhost");
        gridProperties.put("port", "" + portNr);
        gridProperties.put("clientid", "NodeInformation");

        NodeInformation nodeInformation = new NodeInformation("localhost", INSTANCE_ID);
        nodeInformation.addServiceInformation(new ServiceInformation("rest", gridProperties));
        return nodeInformation;
    }

    protected void incrementCityCounter(Map<String, Integer> cityCounters, String counterId) {
        int counter = 0;
        if(cityCounters.containsKey(counterId)) {
            counter = cityCounters.get(counterId);
        }
        counter++;
        cityCounters.put(counterId, counter);
    }

}
