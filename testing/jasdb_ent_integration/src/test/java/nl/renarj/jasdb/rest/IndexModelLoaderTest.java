package nl.renarj.jasdb.rest;

import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.remote.BagConnector;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.client.RestDBSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 *         Date: 8-6-12
 *         Time: 14:15
 */
public class IndexModelLoaderTest extends RestBaseTest {
    @Override
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleKernel.initializeKernel();

        DBSession dbSession = new LocalDBSession();
        EntityBag bag = dbSession.createOrGetBag(BAG_NAME);
        bag.ensureIndex(new IndexField("field", new LongKeyType()), true);
        SimpleKernel.shutdown();

        super.setUp();
    }

    @Test
    public void testRetrieveIndexes() throws RemoteException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        List<IndexDefinition> indexDefinitionList = connector.getIndexDefinitions(new RemotingContext(true), INSTANCE_ID, BAG_NAME);
        assertEquals("There should be two indexes in the list", 1, indexDefinitionList.size());

        IndexDefinition indexDefinition1 = indexDefinitionList.get(0);
        assertTrue("Index name was unexpected", indexDefinition1.getIndexName().equals("field"));
    }

    @Test
    public void testIndexCreate() throws JasDBStorageException {
        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);

        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("fieldx", new StringKeyType()), new ArrayList<IndexField>());
        connector.createIndex(new RemotingContext(true), INSTANCE_ID, BAG_NAME, new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), -1), true);

        List<IndexDefinition> indexDefinitionList = connector.getIndexDefinitions(new RemotingContext(true), INSTANCE_ID, BAG_NAME);
        assertEquals("There should be three indexes in the list", 2, indexDefinitionList.size());

        for(IndexDefinition indexDefinition : indexDefinitionList) {
            String indexName = indexDefinition.getIndexName();
            assertTrue(indexName.equals("field") || indexName.equals("fieldx"));

            if(indexName.equals("fieldx")) {
                assertEquals("Unexpected key header", "fieldx(stringType:1024);", indexDefinition.getHeaderDescriptor());
                assertEquals("Unexpected value header", "__ID(uuidType);", indexDefinition.getValueDescriptor());
                assertEquals("Unexpected index type", 2, indexDefinition.getIndexType());
            }
        }
    }

    @Test
    public void testIndexCreateKey() throws JasDBStorageException, ConfigurationException {
        DBSession session = new RestDBSession(INSTANCE_ID, "localhost", DEFAULT_PORT);
        EntityBag bag = session.createOrGetBag(BAG_NAME);
        bag.ensureIndex(new IndexField("fieldx", new StringKeyType(200)), true);

        NodeInformation nodeInformation = constructNode(DEFAULT_PORT);
        BagConnector connector = RemoteConnectorFactory.createConnector(nodeInformation, BagConnector.class);
        List<IndexDefinition> indexDefinitionList = connector.getIndexDefinitions(new RemotingContext(true), INSTANCE_ID, BAG_NAME);
        assertEquals("There should be three indexes in the list", 2, indexDefinitionList.size());

        for(IndexDefinition indexDefinition : indexDefinitionList) {
            String indexName = indexDefinition.getIndexName();
            assertTrue(indexName.equals("field") || indexName.equals("fieldx"));

            if(indexName.equals("fieldx")) {
                assertEquals("Unexpected key header", "fieldx(stringType:200);", indexDefinition.getHeaderDescriptor());
                assertEquals("Unexpected value header", "__ID(uuidType);", indexDefinition.getValueDescriptor());
                assertEquals("Unexpected index type", 2, indexDefinition.getIndexType());
            }
        }
    }
}
