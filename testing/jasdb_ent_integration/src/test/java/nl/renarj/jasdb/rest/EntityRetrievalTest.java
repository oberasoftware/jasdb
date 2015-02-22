/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.rest;

import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.remote.EntityConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.storage.query.operators.AndBlock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * User: renarj
 * Date: 4/15/12
 * Time: 2:54 PM
 */
public class EntityRetrievalTest extends RestBaseTest {
    private static final Logger log = LoggerFactory.getLogger(EntityRetrievalTest.class);

    private static final int TEST_SIZE = 1000;
    private static final Random rnd = new Random(System.currentTimeMillis());
    private Map<String, Integer> cityMap = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleKernel.initializeKernel();

        DBSession dbSession = new LocalDBSession();
        EntityBag bag = dbSession.createOrGetBag(BAG_NAME);
        bag.ensureIndex(new IndexField("field", new LongKeyType()), true);
        for(int i=0; i<TEST_SIZE; i++) {
            String city = cities[rnd.nextInt(cities.length)];
            bag.addEntity(new SimpleEntity().addProperty("field", i).addProperty("city", city));
            incrementCityCounter(cityMap, city);
        }
        SimpleKernel.shutdown();

        System.setProperty("test.grid.enabled", "true");
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.clearProperty("test.grid.enabled");
    }

    @Test
    public void testRemoteCityRetrieval() throws RemoteException, IOException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);

        for(String city : cities) {
            AndBlock and = new AndBlock();
            and.addCondition("city", new EqualsCondition(new StringKey(city)));

            try (QueryResult result = connector.find(new RemotingContext(true), INSTANCE_ID, BAG_NAME, and, new SearchLimit(), new ArrayList<SortParameter>())) {
                Integer expectedCount = cityMap.get(city);
                assertEquals("Unexpected city count", (long) expectedCount, result.size());
            }
        }
    }

    @Test
    public void testRemoteIdRetrieval() throws RemoteException, IOException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        EntityConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, EntityConnector.class);

        long totalTime = 0;
        for(int i=0; i<TEST_SIZE; i++) {
            AndBlock and = new AndBlock();
            and.addCondition("field", new EqualsCondition(new StringKey("" + i)));

            long start = System.nanoTime();
            try (QueryResult result = connector.find(new RemotingContext(true), INSTANCE_ID, BAG_NAME, and, new SearchLimit(), new ArrayList<SortParameter>())) {
                long end = System.nanoTime();
                totalTime += (end - start);
                assertEquals("There should only be one entity", 1, result.size());
            }
        }
        log.info("Average query operation took: {} ns.", (totalTime / TEST_SIZE));
    }

}
