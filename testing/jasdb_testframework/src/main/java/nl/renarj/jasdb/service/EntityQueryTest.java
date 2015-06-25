/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service;

import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.properties.EntityValue;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.BlockType;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 * Date: 4/28/12
 * Time: 6:27 PM
 */
public abstract class EntityQueryTest extends QueryBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(EntityQueryTest.class);

    private static final QueryBuilder CONTROLLER_QUERY = QueryBuilder.createBuilder()
            .field("controllerId").value("Renzes-MacBook-Pro-2.local")
            .field("type").value("controller");

    private static final QueryBuilder PLUGIN_QUERY = QueryBuilder.createBuilder()
            .field("controllerId").value("Renzes-MacBook-Pro-2.local")
            .field("pluginId").value("zwave")
            .field("type").value("plugin");

    private static final QueryBuilder DEVICE_QUERY = QueryBuilder.createBuilder()
            .field("controllerId").value("Renzes-MacBook-Pro-2.local")
            .field("pluginId").value("zwave")
            .field("type").value("device");

    public EntityQueryTest(DBSessionFactory sessionFactory) {
        super(sessionFactory);
    }

    @Test
    public void testEqualsQuery() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey = "value50";
            String expectedId = valueToId.get(queryKey);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value(queryKey));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field5");
            assertNotNull("Property should be set", property);
            assertTrue("Property should be long", property.getFirstValueObject() instanceof Long);

            long key = property.getFirstValueObject();
            String longExpectedId = longToId.get(key);
            assertEquals("The id's should match", longExpectedId, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testNotEqualsAge() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            for(int age = 0; age < MAX_AGE; age++) {
                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").notEquals(age));
                long start = System.nanoTime();
                long end = System.nanoTime();
                long passed = end - start;
                LOG.info("Query execution took: {}", passed);

                try (QueryResult result = executor.execute()) {
                    int expected = NUMBER_ENTITIES - ageAmounts.get((long) age);

                    for (SimpleEntity entity : result) {
                        assertThat(entity.getValue("age").toString(), not(equalTo(String.valueOf(age))));
                    }
                    assertThat(result.size(), is((long) expected));
                }
            }

        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }


    @Test
    public void testEqualsAndQuery() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey = "value50";
            String expectedId = valueToId.get(queryKey);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value(queryKey).field("field5").value(50));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field5");
            assertNotNull("Property should be set", property);
            assertTrue("Property should be long", property.getFirstValueObject() instanceof Long);

            long key = property.getFirstValueObject();
            String longExpectedId = longToId.get(key);
            assertEquals("The id's should match", longExpectedId, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsNestedEntity() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey = "value50";
            String expectedId = valueToId.get(queryKey);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("embed.embeddedProperty").value(queryKey));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("embed");
            assertNotNull("Property should be set", property);
            assertTrue("Property should be long", property.getFirstValueObject() instanceof EmbeddedEntity);

            EntityValue value = (EntityValue) property.getFirstValue();
            SimpleEntity embedEntity = value.toEntity();

            assertNotNull("Property should be set", embedEntity);

            String embeddedProperty = embedEntity.getProperty("embeddedProperty").getFirstValue().toString();
            assertEquals("The id's should match", queryKey, embeddedProperty);
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsNestedEntityNoIndex() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey = "value50";
            String expectedId = valueToId.get(queryKey);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("embed.embeddedNoIndexProperty").value(queryKey));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("embed");
            assertNotNull("Property should be set", property);
            assertTrue("Property should be long", property.getFirstValueObject() instanceof EmbeddedEntity);

            EntityValue value = (EntityValue) property.getFirstValue();
            SimpleEntity embedEntity = value.toEntity();

            assertNotNull("Property should be set", embedEntity);

            String embeddedProperty = embedEntity.getProperty("embeddedProperty").getFirstValue().toString();
            assertEquals("The id's should match", queryKey, embeddedProperty);
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }

    }


    @Test
    public void testQueryNonExistingProperty() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("thosha");
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013d73bba1f7").addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013d73bba1f8").addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013dad2eefd2").addProperty("type", "contribution"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013dd66f0aed").addProperty("type", "contribution"));

        try {
            QueryBuilder builder = QueryBuilder.createBuilder(BlockType.AND);
            builder.addQueryBlock(QueryBuilder.createBuilder().field("NonExistingProperty").value("00005442-4961-c49d-0000-013dad2eefd2"));
            builder.addQueryBlock(QueryBuilder.createBuilder().field("type").value("contribution"));

            QueryExecutor executor = bag.find(builder);
            try (QueryResult result = executor.execute()) {
                assertThat(result.size(), is(0l));
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }

    }


    @Test
    public void testEqualsOrQuery() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey1 = "value1";
            String queryKey2 = "value50";

            String expectedId1 = valueToId.get(queryKey1);
            String expectedId2 = valueToId.get(queryKey2);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value(queryKey1)
                    .or(QueryBuilder.createBuilder().field("field1").value(queryKey2)).sortBy("field1"));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity1 = result.next();
            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity2 = result.next();

            assertNotNull("There should be a returned entity", entity1);
            assertEquals("The id's should match", expectedId1, entity1.getInternalId());

            assertNotNull("There should be a returned entity", entity2);
            assertEquals("The id's should match", expectedId2, entity2.getInternalId());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testCompoundKeyQuery() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            for(String city : SimpleBaseTest.possibleCities) {
                for(int i=0; i<MAX_AGE; i++) {
                    QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").value(i).field("mainCity").value(city));
                    try (QueryResult result = executor.execute()) {
                        String key = city + "_" + i;
                        if (cityCounters.containsKey(key)) {
                            Long counter = (long) cityCounters.get(city + "_" + i);
                            assertEquals(counter, new Long(result.size()));

                            for (SimpleEntity entity : result) {
                                assertEquals((long) i, (long)entity.getProperty("age").getFirstValueObject());
                                assertEquals(city, entity.getProperty("mainCity").getFirstValueObject());
                            }
                        } else {
                            assertEquals(new Long(0), new Long(result.size()));
                        }
                    }
                }
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsAndQueryExclude() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            String queryKey1 = "value1";
            String queryKey2 = "value50";

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value(queryKey1).field("field1").value(queryKey2));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            LOG.info("Query execution took: {}", passed);

            assertNotNull(result);

            Assert.assertFalse("There should not be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testRangeQuery() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field5").greaterThan(10).field("field5").smallerThan(30));
            try (QueryResult result = executor.execute()) {
                assertEquals(19, result.size());
                assertNotNull(result);

                assertTrue("There should be a result", result.hasNext());
                assertResult(11, 18, result);
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsMultivalueFields() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            for(String city : SimpleBaseTest.possibleCities) {
                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("city").value(city));
                try (QueryResult result = executor.execute()) {
                    assertNotNull(result);
                    assertEquals("Results for city: '" + city + "' are unexpected", new Long(cityCounters.get(city)), new Long(result.size()));
                }
            }

            long totalTime = 0;
            long queries = 0;
            for(String firstCity : SimpleBaseTest.possibleCities) {
                for(String secondCity : SimpleBaseTest.possibleCities) {
                    if(!firstCity.equals(secondCity)) {
                        QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("city").value(firstCity).
                                and(QueryBuilder.createBuilder().field("city").value(secondCity)));
                        long start = System.nanoTime();
                        long end = System.nanoTime();
                        totalTime += (end - start);
                        queries++;

                        try (QueryResult result = executor.execute()) {
                            assertNotNull(result);
                            assertEquals("Results for combined cities: '" + firstCity + ", " + secondCity + "' are unexpected",
                                    new Long(cityCounters.get(firstCity + "_" + secondCity)), new Long(result.size()));
                        }
                    }

                }
            }
            LOG.info("Average query time: {} for {} queries", (totalTime / queries), queries);
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsOutofRange() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            for(String city : SimpleBaseTest.possibleCities) {
                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("city").value(city));
                executor.paging(NUMBER_ENTITIES, 1000);
                try (QueryResult result = executor.execute()) {
                    Assert.assertFalse("There should be no result", result.hasNext());
                }
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }


    /**
     * Simply test if we can query with an empty string key
     */
    @Test
    public void testEmptyStringKey() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value(""));

            try (QueryResult result = executor.execute()) {
                assertNotNull(result);
                Assert.assertFalse("There should not be a result", result.hasNext());
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }


    @Test
    public void testKeySpecialCharacters() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        bag.addEntity(new SimpleEntity().addProperty("field1", "coëfficiënt van Poisson"));

        try {
            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field1").value("coëfficiënt van Poisson"));
            QueryResult result = executor.execute();
            assertTrue(result.hasNext());
            SimpleEntity entity = result.next();
            assertEquals("coëfficiënt van Poisson", entity.getProperty("field1").getFirstValueObject());
            assertFalse(result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testPartialIndexes() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("homeautotest");

        bag.ensureIndex(
                new CompositeIndexField(
                        new IndexField("controllerId", new StringKeyType()),
                        new IndexField("pluginId", new StringKeyType()),
                        new IndexField("deviceId", new StringKeyType()),
                        new IndexField("type", new StringKeyType())
                ), false);

        try {
            String controllerEntityId = bag.addEntity(new SimpleEntity().addProperty("controllerId", "Renzes-MacBook-Pro-2.local")
                    .addProperty("plugins", "7158f3ec-681f-4d9b-9ab1-6ab6c60288e3")
                    .addProperty("type", "controller")).getInternalId();

            String pluginEntityId = bag.addEntity(new SimpleEntity()
                    .addProperty("controllerId", "Renzes-MacBook-Pro-2.local").addProperty("pluginId", "zwave")
                    .addProperty("name", "ZWave provider")
                    .addProperty("type", "plugin")).getInternalId();

            String deviceEntityId = bag.addEntity(new SimpleEntity()
                    .addProperty("controllerId", "Renzes-MacBook-Pro-2.local").addProperty("pluginId", "zwave")
                    .addProperty("name", "ZWave provider")
                    .addProperty("deviceId", "13")
                    .addProperty("type", "device")).getInternalId();


            //verify we can query after insertion
            assertPartialIndexItems(bag, pluginEntityId, deviceEntityId, controllerEntityId);


            bag.updateEntity(new SimpleEntity(controllerEntityId).addProperty("controllerId", "Renzes-MacBook-Pro-2.local")
                    .addProperty("plugins", "7158f3ec-681f-4d9b-9ab1-6ab6c60288e3").addProperty("type", "controller"));

            bag.updateEntity(new SimpleEntity(pluginEntityId)
                    .addProperty("controllerId", "Renzes-MacBook-Pro-2.local").addProperty("pluginId", "zwave")
                    .addProperty("name", "ZWave provider").addProperty("type", "plugin"));

            //verify it still works after an update operation
            assertPartialIndexItems(bag, pluginEntityId, deviceEntityId, controllerEntityId);
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    private void assertPartialIndexItems(EntityBag bag, String pluginId, String deviceId, String controllerId) throws JasDBStorageException {

        List<SimpleEntity> plugins = getEntities(bag, PLUGIN_QUERY);
        assertThat(plugins.size(), is(1));
        assertThat(getEntityValue(plugins, SimpleEntity.DOCUMENT_ID), hasItems(pluginId));

        List<SimpleEntity> controllers = getEntities(bag, CONTROLLER_QUERY);
        assertThat(controllers.size(), is(1));
        assertThat(getEntityValue(controllers, SimpleEntity.DOCUMENT_ID), hasItems(controllerId));

        List<SimpleEntity> devices = getEntities(bag, DEVICE_QUERY);
        assertThat(devices.size(), is(1));
        assertThat(getEntityValue(devices, SimpleEntity.DOCUMENT_ID), hasItems(deviceId));
    }



}
