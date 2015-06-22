package nl.renarj.jasdb.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public abstract class DBSessionTest {
    private DBSessionFactory sessionFactory;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void tearDown() throws Exception {
        SimpleKernel.shutdown();
        SimpleBaseTest.cleanData();
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleBaseTest.cleanData();
    }

    protected DBSessionTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Test
    public void testGetInstances() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        List<Instance> instanceList = session.getInstances();
        assertEquals(1, instanceList.size());

        Instance instance = instanceList.get(0);
        assertEquals("default", instance.getInstanceId());
        assertEquals(SimpleBaseTest.jasdbDir, new File(instance.getPath()));
    }

    @Test
    public void addInstance() throws JasDBStorageException, IOException {
        File instanceFolder = temporaryFolder.newFolder();
        DBSession session = sessionFactory.createSession();
        session.addInstance("myInstance", instanceFolder.toString());

        List<Instance> instanceList = session.getInstances();
        assertEquals(2, instanceList.size());

        session.createOrGetBag("myInstance", "testbag");
        assertTrue(new File(instanceFolder, "testbag.pjs").exists());
    }

    @Test
    public void addSessionInstanceBound() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        assertEquals("default", session.getInstanceId());
        session.createOrGetBag("testbag");
        assertEquals(1, session.getBags().size());

        session.addInstance("myInstance", temporaryFolder.newFolder().toString());
        assertEquals("default", session.getInstanceId());

        DBSession newSession = sessionFactory.createSession("myInstance");
        newSession.createOrGetBag("bag1");
        newSession.createOrGetBag("bag2");
        assertEquals("myInstance", newSession.getInstanceId());
        assertEquals(2, newSession.getBags().size());

        assertEquals(1, session.getBags().size());
    }

    @Test
    public void testCreateAndGetInstanceBag() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance("myInstance", temporaryFolder.newFolder().toString());

        session.createOrGetBag("myInstance", "bag1");

        //default instance should be null
        assertNull(session.getBag("bag1"));
        assertNotNull("myInstance", session.getBag("myInstance", "bag1"));
    }

    @Test
    public void testCreateAndInsertEntities() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance("myInstance", temporaryFolder.newFolder().toString());

        EntityBag bag = session.createOrGetBag("myInstance", "bag1");
        bag.addEntity(new SimpleEntity().addProperty("test", "value"));

        QueryResult result = bag.getEntities();
        assertThat(result.size(), is(1l));
        SimpleEntity entity = result.next();
        assertThat(entity, notNullValue());
        assertThat(entity.getProperty("test").getFirstValue().toString(), is("value"));
    }

    @Test
    public void testSwitchInstance() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addInstance("myInstance", temporaryFolder.newFolder().toString());
        assertEquals("default", session.getInstanceId());

        session.switchInstance("myInstance");
        assertEquals("myInstance", session.getInstanceId());

        session.addAndSwitchInstance("anotherInstance", temporaryFolder.newFolder().toString());
        assertEquals("anotherInstance", session.getInstanceId());
    }

    @Test(expected = JasDBStorageException.class)
    public void testGetNonExistingInstance() throws JasDBStorageException, IOException {
        sessionFactory.createSession("myNotExistingInstance");
    }

    @Test
    public void testDeleteInstance() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        File instanceFolder = temporaryFolder.newFolder();
        session.addAndSwitchInstance("myInstance", instanceFolder.toString());

        assertEquals("myInstance", session.getInstanceId());
        session.createOrGetBag("bag1");
        session.createOrGetBag("bag2");
        assertTrue(new File(instanceFolder, "bag1.pjs").exists());
        assertTrue(new File(instanceFolder, "bag2.pjs").exists());

        session.switchInstance("default");

        session.deleteInstance("myInstance");
        assertFalse(new File(instanceFolder, "bag1.pjs").exists());
        assertFalse(new File(instanceFolder, "bag2.pjs").exists());
    }

    @Test
    public void testGetBagList() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        session.addAndSwitchInstance("myInstance", temporaryFolder.newFolder().toString());

        session.createOrGetBag("bag1");
        session.createOrGetBag("bag2");

        assertEquals(2, session.getBags().size());
        assertEquals(0, session.getBags("default").size());
        assertEquals(2, session.getBags("myInstance").size());
    }

    @Test
    public void testBagCreateOrGet() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        session.createOrGetBag("testbag1");

        assertTrue(new File(SimpleBaseTest.jasdbDir, "testbag1.pjs").exists());
        assertEquals(1, session.getBags().size());
    }

    @Test
    public void testBagGetNonExisting() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        assertNull(session.getBag("testbag1"));
        assertNull(session.getBag("testbag2"));
        assertEquals(0, session.getBags().size());

        assertFalse(new File(SimpleBaseTest.tmpDir, "testbag1.pjs").exists());
        assertFalse(new File(SimpleBaseTest.tmpDir, "testbag1.pjsm").exists());

        assertFalse(new File(SimpleBaseTest.tmpDir, "testbag2.pjs").exists());
        assertFalse(new File(SimpleBaseTest.tmpDir, "testbag2.pjsm").exists());
    }

    @Test
    public void testBagRemove() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag1");
        bag.ensureIndex(new IndexField("field1", new StringKeyType()), false);
        assertTrue(new File(SimpleBaseTest.jasdbDir, "testbag1.pjs").exists());
        assertTrue(new File(SimpleBaseTest.jasdbDir, "testbag1_field1ID.idx").exists());
        assertEquals(1, session.getBags().size());

        session.removeBag("testbag1");
        assertFalse(new File(SimpleBaseTest.jasdbDir, "testbag1.pjs").exists());
        assertFalse(new File(SimpleBaseTest.jasdbDir, "testbag1_field1ID.idx").exists());
    }

    @Test
    public void testBagRemoveInstance() throws JasDBStorageException, IOException {
        DBSession session = sessionFactory.createSession();
        File newInstanceFolder = temporaryFolder.newFolder();
        session.addAndSwitchInstance("myInstance", newInstanceFolder.toString());

        EntityBag bag = session.createOrGetBag("testbag1");
        bag.ensureIndex(new IndexField("field1", new StringKeyType()), false);
        assertTrue(new File(newInstanceFolder, "testbag1.pjs").exists());
        assertTrue(new File(newInstanceFolder, "testbag1_field1ID.idx").exists());
        assertEquals(1, session.getBags().size());

        session.removeBag("testbag1");
        assertFalse(new File(newInstanceFolder, "testbag1.pjs").exists());
        assertFalse(new File(newInstanceFolder, "testbag1_field1ID.idx").exists());
    }

    @Test
    public void testEntityManagerPerist() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();

        String id = UUID.randomUUID().toString();
        TestEntity entity = new TestEntity(id, "Renze", "de Vries", Lists.newArrayList("programming", "model building", "biking"),
                new ImmutableMap.Builder<String, String>()
                        .put("city", "Amsterdam")
                        .put("street", "Secret passageway 10")
                        .put("zipcode", "0000TT").build());
        assertThat(entityManager.persist(entity).getInternalId(), is(id));

        EntityBag testBag = session.createOrGetBag("TEST_BAG");
        assertThat(testBag.getSize(), is(1l));
        SimpleEntity mappedEntity = testBag.getEntity(id);

        assertThat(mappedEntity.getValue("firstName"), is("Renze"));
        assertThat(mappedEntity.getValue("lastName"), is("de Vries"));
        assertThat(mappedEntity.getValues("HobbyList"), hasItems("programming", "model building", "biking"));

        SimpleEntity addressEntity = mappedEntity.getEntity("Address");
        assertThat(addressEntity.getValue("city"), is("Amsterdam"));
        assertThat(addressEntity.getValue("street"), is("Secret passageway 10"));
        assertThat(addressEntity.getValue("zipcode"), is("0000TT"));
    }

    @Test
    public void testEntityManagerUpdate() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();

        String id = UUID.randomUUID().toString();

        TestEntity entity = new TestEntity(id, "Renze", "de Vries", Lists.newArrayList("programming", "model building", "biking"),
                new ImmutableMap.Builder<String, String>()
                        .put("city", "Amsterdam")
                        .put("street", "Secret passageway 10")
                        .put("zipcode", "0000TT").build());
        assertThat(entityManager.persist(entity).getInternalId(), is(id));

        EntityBag testBag = session.createOrGetBag("TEST_BAG");
        assertThat(testBag.getSize(), is(1l));
        SimpleEntity mappedEntity = testBag.getEntity(id);

        assertThat(mappedEntity.getValue("firstName"), is("Renze"));
        assertThat(mappedEntity.getValue("lastName"), is("de Vries"));


        entity.setFirstName("Updated");
        entityManager.persist(entity);

        mappedEntity = testBag.getEntity(id);

        assertThat(mappedEntity.getValue("firstName"), is("Updated"));
        assertThat(mappedEntity.getValue("lastName"), is("de Vries"));
    }

}
