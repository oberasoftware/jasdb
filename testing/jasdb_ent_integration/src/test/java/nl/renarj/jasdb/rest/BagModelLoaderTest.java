package nl.renarj.jasdb.rest;

import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.engine.metadata.BagMeta;
import com.oberasoftware.jasdb.service.local.LocalDBSession;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import com.oberasoftware.jasdb.core.metadata.Bag;
import com.oberasoftware.jasdb.core.metadata.IndexDefinition;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.BagConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.remote.model.RemoteBag;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Renze de Vries
 */
public class BagModelLoaderTest extends RestBaseTest {
    @Override
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleKernel.initializeKernel();

        DBSession dbSession = new LocalDBSession();
        dbSession.createOrGetBag(BAG_NAME);
        SimpleKernel.shutdown();

        super.setUp();
    }

    @Test
    public void testListBags() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        List<RemoteBag> bagMetaList = connector.getBags(new RemotingContext(true), INSTANCE_ID);
        assertEquals("There should be one bag definition", 1, bagMetaList.size());
        assertEquals("Unepxected bag name", BAG_NAME, bagMetaList.get(0).getName());
    }

    @Test
    public void testGetBagNonExisting() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        RemoteBag foundBag = connector.getBag(new RemotingContext(true), INSTANCE_ID, "TESTBAG2");
        assertNull("There should not be a found bag", foundBag);
    }

    @Test
    public void testGetExistingBag() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        RemoteBag foundBag = connector.getBag(new RemotingContext(true), INSTANCE_ID, BAG_NAME);
        assertNotNull(foundBag);
        assertEquals("Unepxected bag name", BAG_NAME, foundBag.getName());
    }

    @Test
    public void testCreateNewBag() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        List<RemoteBag> bagMetaList = connector.getBags(new RemotingContext(true), INSTANCE_ID);
        assertEquals("There should be one bag definition", 1, bagMetaList.size());

        Bag bagMeta = connector.createBag(new RemotingContext(true), INSTANCE_ID, new BagMeta(INSTANCE_ID, "TESTBAG2", new ArrayList<IndexDefinition>()));
        assertEquals("Unexpected bagname", "TESTBAG2", bagMeta.getName());
        assertEquals("Unexpected instanceId", INSTANCE_ID, bagMeta.getInstanceId());

        bagMetaList = connector.getBags(new RemotingContext(true), INSTANCE_ID);
        assertEquals("There should be one bag definition", 2, bagMetaList.size());
    }

    @Test(expected = RemoteException.class)
    public void testCreateNewBagNoName() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        connector.createBag(new RemotingContext(true), INSTANCE_ID, new BagMeta(INSTANCE_ID, null, new ArrayList<IndexDefinition>()));
    }

}
