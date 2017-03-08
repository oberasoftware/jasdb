package com.oberasoftware.jasdb.service;

import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.service.local.ApplicationContextProvider;
import com.oberasoftware.jasdb.service.local.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.engine.IndexManager;
import nl.renarj.jasdb.api.engine.IndexManagerFactory;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author Renze de Vries
 */
public class IndexRebuildTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String storageLocation;

    @Before
    public void setUp() throws IOException {
        storageLocation = temporaryFolder.newFolder().toString();
        System.setProperty(HomeLocatorUtil.JASDB_HOME, storageLocation);
    }

    @After
    public void tearDown() throws JasDBException {
        JasDBMain.shutdown();
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

        IndexManagerFactory indexManagerFactory = ApplicationContextProvider.getApplicationContext().getBean(IndexManagerFactory.class);
        IndexManager indexManager = indexManagerFactory.getIndexManager("default");
        Index index = indexManager.getIndex("bag0", "field1");

        assertNotNull(index);

        for(int i=0; i<testSize; i++) {
            assertFalse("There should be a result", index.searchIndex(new EqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).isEmpty());
        }
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
            JasDBMain.shutdown();
        }

        assertTrue(new File(storageLocation, "metadata.pid").createNewFile());
        //lets do a brute force trick to remove the index
        assertThat(new File(storageLocation, "bag0_field1.idx").delete(), is(true));

        JasDBMain.start();

        IndexManagerFactory indexManagerFactory = ApplicationContextProvider.getApplicationContext().getBean(IndexManagerFactory.class);
        IndexManager indexManager = indexManagerFactory.getIndexManager("default");
        Index index = indexManager.getIndex("bag0", "field1");
        assertNotNull(index);

        for(int i=0; i<testSize; i++) {
            assertFalse("There should be a result", index.searchIndex(new EqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).isEmpty());
        }
    }
}
