package nl.renarj.jasdb.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.entitymapper.EntityManager;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public abstract class EntityManagerTest extends QueryBaseTest {
    public EntityManagerTest(DBSessionFactory sessionFactory) {
        super(sessionFactory);
    }

    @Override
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleBaseTest.cleanData();
    }

    @Test
    public void testEntityManagerUpdate() throws JasDBStorageException {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();

        String id = UUID.randomUUID().toString();

        TestEntity entity = new TestEntity(id, "Renze", "de Vries", newArrayList("programming", "model building", "biking"),
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


    @Test
    public void testFindAll() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();
        try {
            entityManager.persist(new TestEntity(null, "Piet", "de Klos", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjaak", "Gaar", newArrayList("darting", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Klaas", "Vaak", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjors", "Pineut", newArrayList("cool stuff", "darting"), new HashMap<>()));

            List<TestEntity> testEntities = entityManager.findEntities(TestEntity.class, QueryBuilder.createBuilder());
            assertThat(testEntities.size(), is(4));

            List<String> names = testEntities.stream().map(TestEntity::getFirstName).collect(Collectors.toList());
            assertThat(names, hasItems("Piet", "Sjaak", "Klaas", "Sjors"));
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testFindByName() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();
        try {
            String id1 = entityManager.persist(new TestEntity(null, "Piet", "de Klos",
                    newArrayList("cool stuff", "programming for dummies"), new HashMap<>())).getInternalId();
            String id2 = entityManager.persist(new TestEntity(null, "Sjaak", "Gaar",
                    newArrayList("darting", "programming for dummies"), new HashMap<>())).getInternalId();
            String id3 = entityManager.persist(new TestEntity(null, "Klaas", "Vaak",
                    newArrayList("cool stuff", "programming for dummies"), new HashMap<>())).getInternalId();
            String id4 = entityManager.persist(new TestEntity(null, "Sjors", "Pineut",
                    newArrayList("cool stuff", "darting"), new HashMap<>())).getInternalId();

            QueryBuilder builder1 = QueryBuilder.createBuilder().field("firstName").value("Piet");
            QueryBuilder builder2 = QueryBuilder.createBuilder().field("firstName").value("Sjaak");
            QueryBuilder builder3 = QueryBuilder.createBuilder().field("lastName").value("Vaak");
            QueryBuilder builder4 = QueryBuilder.createBuilder().field("lastName").value("Pineut");

            assertHasExactlyOneId(entityManager, builder1, id1);
            assertHasExactlyOneId(entityManager, builder2, id2);
            assertHasExactlyOneId(entityManager, builder3, id3);
            assertHasExactlyOneId(entityManager, builder4, id4);
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testLimit() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();
        try {
            entityManager.persist(new TestEntity(null, "Piet", "de Klos", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjaak", "Gaar", newArrayList("darting", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Klaas", "Vaak", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjors", "Pineut", newArrayList("cool stuff", "darting"), new HashMap<>()));

            List<TestEntity> testEntities = entityManager.findEntities(TestEntity.class, QueryBuilder.createBuilder(), 2);
            assertThat(testEntities.size(), is(2));

            List<String> names = testEntities.stream().map(TestEntity::getFirstName).collect(Collectors.toList());
            assertThat(names, hasItems("Piet", "Sjaak"));
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testPaging() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityManager entityManager = session.getEntityManager();
        try {
            entityManager.persist(new TestEntity(null, "Piet", "de Klos", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjaak", "Gaar", newArrayList("darting", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Klaas", "Vaak", newArrayList("cool stuff", "programming for dummies"), new HashMap<>()));
            entityManager.persist(new TestEntity(null, "Sjors", "Pineut", newArrayList("cool stuff", "darting"), new HashMap<>()));

            List<TestEntity> testEntities = entityManager.findEntities(TestEntity.class, QueryBuilder.createBuilder(), 0, 2);
            assertThat(testEntities.size(), is(2));
            List<String> names = testEntities.stream().map(TestEntity::getFirstName).collect(Collectors.toList());
            assertThat(names, hasItems("Piet", "Sjaak"));

            testEntities = entityManager.findEntities(TestEntity.class, QueryBuilder.createBuilder(), 2, 2);
            assertThat(testEntities.size(), is(2));
            names = testEntities.stream().map(TestEntity::getFirstName).collect(Collectors.toList());
            assertThat(names, hasItems("Klaas", "Sjors"));
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    private void assertHasExactlyOneId(EntityManager entityManager, QueryBuilder query, String id) throws JasDBStorageException{
        assertResults(entityManager, query, 1, Lists.newArrayList(id));
    }

    private void assertResults(EntityManager entityManager, QueryBuilder query, int results, List<String> ids) throws JasDBStorageException {
        List<TestEntity> entities = entityManager.findEntities(TestEntity.class, query);
        assertThat(entities.size(), is(results));

        List<String> foundIds = entities.stream().map(TestEntity::getId).collect(Collectors.toList());
        assertThat(foundIds, hasItems(ids.toArray(new String[ids.size()])));

    }
}
