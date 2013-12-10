package nl.renarj.storage;

import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.service.StorageServiceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class IndexRebuildTest extends DBBaseTest {
    @Before
    public void setUp() {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());

        cleanData();
    }

    @After
    public void tearDown() throws JasDBException {
        super.tearDown();
        cleanData();
    }

    @Test
    public void testIndexCreateExistingData() throws Exception {
        int testSize = 10000;
        DBSession session = new LocalDBSession();
        EntityBag bag = session.createOrGetBag("bag0");
        for(int i=0; i<testSize; i++) {
            bag.addEntity(new SimpleEntity().addProperty("field1", (long) i));
        }
        bag.ensureIndex(new IndexField("field1", new LongKeyType()), true);

        Thread.sleep(10000);

        StorageServiceFactory serviceFactory = SimpleKernel.getStorageServiceFactory();
//        IndexManager indexManager = serviceFactory.getIndexManager(SimpleKernel.getInstanceFactory().getInstance());
//        Index index = indexManager.getIndex("bag0", "field1");
//        assertNotNull(index);
//
//        for(int i=0; i<testSize; i++) {
//            assertFalse("There should be a result", index.searchIndex(new EqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).isEmpty());
//        }
    }

    @Test
    public void testIndexStartCorrupt() throws Exception {
        int testSize = 10000;
        DBSession session = new LocalDBSession();
        try {
            EntityBag bag = session.createOrGetBag("bag0");
            bag.ensureIndex(new IndexField("field1", new LongKeyType()), true);
            for(int i=0; i<testSize; i++) {
                bag.addEntity(new SimpleEntity().addProperty("field1", (long) i));
            }
        } finally {
            SimpleKernel.shutdown();
        }

        assertTrue(new File(jasdbDir, "metadata.pid").createNewFile());
        //lets do a brute force trick to remove the index
        assertDelete(new File(jasdbDir, "bag0_field1.idx"));

        SimpleKernel.initializeKernel();

        StorageServiceFactory serviceFactory = SimpleKernel.getStorageServiceFactory();
//        IndexManager indexManager = serviceFactory.getIndexManager(SimpleKernel.getInstanceFactory().getInstance());
//        Index index = indexManager.getIndex("bag0", "field1");
//        assertNotNull(index);
//
//        for(int i=0; i<testSize; i++) {
//            assertFalse("There should be a result", index.searchIndex(new EqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).isEmpty());
//        }
    }
}
