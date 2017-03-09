package com.oberasoftware.jasdb.test;

import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.service.JasDBMain;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
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

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public abstract class DBSessionTest {
    private static final Logger LOG = getLogger(DBSessionTest.class);

    private static final String MY_INSTANCE = "myInstance";
    private static final String BAG_1 = "bag1";
    private DBSessionFactory sessionFactory;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String storageLocation;
    private String jasdbHome;

    @After
    public void tearDown() throws Exception {
        JasDBMain.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        storageLocation = temporaryFolder.newFolder().toString();
        jasdbHome = storageLocation + "/.jasdb";
        System.setProperty(HomeLocatorUtil.JASDB_HOME, storageLocation);
        JasDBMain.start();
    }

    protected DBSessionTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Test
    public void testGetInstances() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        List<Instance> instanceList = session.getInstances();
        assertEquals(1, instanceList.size());

        Instance instance = instanceList.get(0);
        assertEquals("default", instance.getInstanceId());
        assertEquals(jasdbHome, new File(instance.getPath()).toString());
    }

    @Test
    public void addInstance() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance(MY_INSTANCE);

        List<Instance> instanceList = session.getInstances();
        assertEquals(2, instanceList.size());

        session.createOrGetBag(MY_INSTANCE, "testbag");
        File instanceLocation = new File(storageLocation + "/.jasdb/myInstance");
        assertThat(instanceLocation.exists(), is(true));
        assertThat(new File(instanceLocation, "testbag.pjs").exists(), is(true));
    }

    @Test
    public void addSessionInstanceBound() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        assertEquals("default", session.getInstanceId());
        session.createOrGetBag("testbag");
        assertEquals(1, session.getBags().size());

        session.addInstance(MY_INSTANCE);
        assertEquals("default", session.getInstanceId());

        DBSession newSession = sessionFactory.createSession(MY_INSTANCE);
        newSession.createOrGetBag(BAG_1);
        newSession.createOrGetBag("bag2");
        assertEquals(MY_INSTANCE, newSession.getInstanceId());
        assertEquals(2, newSession.getBags().size());

        assertEquals(1, session.getBags().size());
    }

    @Test
    public void testCreateAndGetInstanceBag() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance(MY_INSTANCE);

        session.createOrGetBag(MY_INSTANCE, BAG_1);

        //default instance should be null
        assertNull(session.getBag(BAG_1));
        assertNotNull(MY_INSTANCE, session.getBag(MY_INSTANCE, BAG_1));
    }

    @Test
    public void testCreateAndInsertEntities() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance(MY_INSTANCE);

        EntityBag bag = session.createOrGetBag(MY_INSTANCE, BAG_1);
        bag.addEntity(new SimpleEntity().addProperty("test", "value"));

        QueryResult result = bag.getEntities();
        assertThat(result.size(), is(1l));
        SimpleEntity entity = result.next();
        assertThat(entity, notNullValue());
        assertThat(entity.getProperty("test").getFirstValue().toString(), is("value"));
    }

    @Test
    public void testSwitchInstance() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance(MY_INSTANCE);
        assertEquals("default", session.getInstanceId());

        session.switchInstance(MY_INSTANCE);
        assertEquals(MY_INSTANCE, session.getInstanceId());

        session.addAndSwitchInstance("anotherInstance");
        assertEquals("anotherInstance", session.getInstanceId());
    }

    @Test(expected = JasDBException.class)
    public void testGetNonExistingInstance() throws JasDBException, IOException {
        sessionFactory.createSession("myNotExistingInstance");
    }

    @Test
    public void testDeleteInstance() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        File instanceFolder = new File(storageLocation + "/.jasdb/myInstance");
        session.addAndSwitchInstance(MY_INSTANCE);

        assertEquals(MY_INSTANCE, session.getInstanceId());
        session.createOrGetBag(BAG_1);
        session.createOrGetBag("bag2");
        assertTrue(new File(instanceFolder, "bag1.pjs").exists());
        assertTrue(new File(instanceFolder, "bag2.pjs").exists());

        session.switchInstance("default");

        session.deleteInstance(MY_INSTANCE);
        assertFalse(new File(instanceFolder, "bag1.pjs").exists());
        assertFalse(new File(instanceFolder, "bag1.idx").exists());
        assertFalse(new File(instanceFolder, "bag2.pjs").exists());
        assertFalse(new File(instanceFolder, "bag2.idx").exists());
    }

    @Test
    public void testGetBagList() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addAndSwitchInstance(MY_INSTANCE);

        session.createOrGetBag(BAG_1);
        session.createOrGetBag("bag2");

        assertEquals(2, session.getBags().size());
        assertEquals(0, session.getBags("default").size());
        assertEquals(2, session.getBags(MY_INSTANCE).size());
    }

    @Test
    public void testBagCreateOrGet() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        session.createOrGetBag("testbag1");

        assertTrue(new File(jasdbHome, "testbag1.pjs").exists());
        assertEquals(1, session.getBags().size());
    }

    @Test
    public void testBagGetNonExisting() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        assertNull(session.getBag("testbag1"));
        assertNull(session.getBag("testbag2"));
        assertEquals(0, session.getBags().size());

        assertFalse(new File(jasdbHome, "testbag1.pjs").exists());
        assertFalse(new File(jasdbHome, "testbag1.pjsm").exists());

        assertFalse(new File(jasdbHome, "testbag2.pjs").exists());
        assertFalse(new File(jasdbHome, "testbag2.pjsm").exists());
    }

    @Test
    public void testBagRemove() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag1");
        bag.ensureIndex(new IndexField("field1", new StringKeyType()), false);
        assertTrue(new File(jasdbHome, "testbag1.pjs").exists());
        assertTrue(new File(jasdbHome, "testbag1_field1ID.idx").exists());
        assertEquals(1, session.getBags().size());

        session.removeBag("testbag1");
        assertFalse(new File(jasdbHome, "testbag1.pjs").exists());
        assertFalse(new File(jasdbHome, "testbag1_field1ID.idx").exists());
    }

    @Test
    public void testBagRemoveInstance() throws JasDBException, IOException {
        DBSession session = sessionFactory.createSession();
        File newInstanceFolder = new File(storageLocation + "/.jasdb/myInstance");
        session.addAndSwitchInstance(MY_INSTANCE);

        EntityBag bag = session.createOrGetBag("testbag1");
        bag.ensureIndex(new IndexField("field1", new StringKeyType()), false);
        assertTrue(new File(newInstanceFolder, "testbag1.pjs").exists());
        assertTrue(new File(newInstanceFolder, "testbag1_field1ID.idx").exists());
        assertEquals(1, session.getBags().size());

        session.removeBag("testbag1");
        assertFalse(new File(newInstanceFolder, "testbag1.pjs").exists());
        assertFalse(new File(newInstanceFolder, "testbag1_field1ID.idx").exists());
    }
}
