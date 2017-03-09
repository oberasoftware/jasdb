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
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public class IndexRebuildTest {
    private static final Logger LOG = getLogger(IndexRebuildTest.class);

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
        String jasdbHome = storageLocation + "/.jasdb";

        assertThat(new File(jasdbHome, "metadata.pid").createNewFile(), is(true));
        assertThat(new File(jasdbHome, "bag0_field1.idx").delete(), is(true));

        JasDBMain.start();
        new LocalDBSession().getBag("bag0").getEntities();
        sleepUninterruptibly(5, TimeUnit.SECONDS);
        LOG.info("Expect that index has been repaired");

        IndexManagerFactory indexManagerFactory = ApplicationContextProvider.getApplicationContext().getBean(IndexManagerFactory.class);
        IndexManager indexManager = indexManagerFactory.getIndexManager("default");
        Index index = indexManager.getIndex("bag0", "field1");
        assertNotNull(index);

        for(int i=0; i<testSize; i++) {
            List<Key> foundKeys = index.searchIndex(new EqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).getKeys();
            assertThat(foundKeys, hasItems(new LongKey(i)));
        }
    }
}
