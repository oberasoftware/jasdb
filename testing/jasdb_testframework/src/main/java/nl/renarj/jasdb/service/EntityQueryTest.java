/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.properties.EntityValue;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.*;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * @author Renze de Vries
 * Date: 4/28/12
 * Time: 6:27 PM
 */
public abstract class EntityQueryTest {
    private Logger log = LoggerFactory.getLogger(EntityQueryTest.class);
    private static final int NUMBER_ENTITIES = 1000;
    private static final int MAX_AGE = 50;

    private Map<Long, String> longToId = new HashMap<>();
    private Map<String, String> valueToId = new HashMap<>();
    private Map<Long, Integer> ageAmounts = new HashMap<>();
    private Map<String, Integer> cityCounters = new HashMap<>();

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



    private DBSessionFactory sessionFactory;

    protected EntityQueryTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @After
    public void tearDown() throws Exception {
        SimpleKernel.shutdown();
        SimpleBaseTest.cleanData();
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, SimpleBaseTest.tmpDir.toString());
        SimpleBaseTest.cleanData();

        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        bag.ensureIndex(new IndexField("field1", new StringKeyType()), false);
        bag.ensureIndex(new IndexField("field5", new LongKeyType()), false, new IndexField("field6", new LongKeyType()));
        bag.ensureIndex(new IndexField("field6", new LongKeyType()), false);
        bag.ensureIndex(new IndexField("age", new LongKeyType()), false);
        bag.ensureIndex(new IndexField("city", new StringKeyType(200)), false);
        bag.ensureIndex(new IndexField("embed.embeddedProperty", new StringKeyType(100)), false);
        bag.ensureIndex(new CompositeIndexField(new IndexField("age", new LongKeyType()), new IndexField("mainCity", new StringKeyType(200))), false);

        Random rnd = new Random(System.currentTimeMillis());
        for(int i=0; i< NUMBER_ENTITIES; i++) {
            String value = "value" + i;
            Long lValue = (long) i;
            String fieldValue = "myValue" + i;
            Long age = (long) rnd.nextInt(MAX_AGE);

            String city1 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];

            String city2 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];
            while(city2.equals(city1)) {
                city2 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];
            }

            EmbeddedEntity embeddedEntity = new EmbeddedEntity();
            embeddedEntity.addProperty("embeddedProperty", value);
            embeddedEntity.addProperty("embeddedNoIndexProperty", value);

            SimpleEntity entity = bag.addEntity(new SimpleEntity()
                    .addProperty("field1", value)
                    .addProperty("field5", lValue)
                    .addProperty("field6", Long.valueOf(NUMBER_ENTITIES - i))
                    .addProperty("field7", fieldValue)
                    .addProperty("field8", fieldValue)
                    .addProperty("field9", lValue)
                    .addEntity("embed", embeddedEntity)
                    .addProperty("age", age)
                    .addProperty("mainCity", city1)
                    .addProperty("city", city1, city2)
            );
            incrementCityCounter(city1);
            incrementCityCounter(city2);
            incrementCityCounter(city1 + "_" + city2);
            incrementCityCounter(city2 + "_" + city1);
            incrementCityCounter(city1 + "_" + age);

            int amount = 1;
            if(ageAmounts.containsKey(age)) {
                amount = ageAmounts.get(age);
                amount++;
            }
            ageAmounts.put(age, amount);

            longToId.put(lValue, entity.getInternalId());
            valueToId.put(value, entity.getInternalId());
        }
    }

    private void incrementCityCounter(String counterId) {
        int counter = 0;
        if(cityCounters.containsKey(counterId)) {
            counter = cityCounters.get(counterId);
        }
        counter++;
        cityCounters.put(counterId, counter);
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
            log.info("Query execution took: {}", passed);

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
                log.info("Query execution took: {}", passed);

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
    public void testSortDescendingInvalidType() throws Exception {
        DBSession session = sessionFactory.createSession();
        try{
            EntityBag bag = session.createOrGetBag("Bag");

            SimpleEntity entity = new SimpleEntity();
            entity.addProperty("name", "xxx");
            entity.addProperty("v", "3");
            bag.addEntity(entity);

            entity = new SimpleEntity();
            entity.addProperty("name", 1);
            entity.addProperty("v", "1");
            bag.addEntity(entity);

            entity = new SimpleEntity();
            entity.addProperty("name", "xxx");
            entity.addProperty("v", "2");
            bag.addEntity(entity);

            QueryBuilder innerQuery = QueryBuilder.createBuilder();
            innerQuery.field("name").value("xxx").sortBy("v",Order.DESCENDING);

            QueryExecutor executor = bag.find(innerQuery);
            QueryResult result = executor.execute();
            assertThat(result.size(), is(2l));

            assertThat((String)result.next().getValue("v"), is("3"));
            assertThat((String)result.next().getValue("v"), is("2"));
        } finally {
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testSortByNonExistingField() throws Exception {
        DBSession session = sessionFactory.createSession();
        try{
            EntityBag bag = session.createOrGetBag("Bag");

            SimpleEntity entity = new SimpleEntity();
            entity.addProperty("name", "xxx");
            entity.addProperty("v", "1");
            bag.addEntity(entity);

            entity = new SimpleEntity();
            entity.addProperty("name", "xxx");
            entity.addProperty("v", "2");
            bag.addEntity(entity);


            QueryBuilder innerQuery = QueryBuilder.createBuilder();
            innerQuery.field("name").value("xxx").sortBy("_id",Order.DESCENDING).sortBy("id",Order.DESCENDING);

            QueryExecutor executor = bag.find(innerQuery);
            QueryResult result = executor.execute();
            assertThat(result.size(), is(2l));
        } finally {
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsAgeWithLimiting() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            int limit = 3;
            Integer maxAmount = ageAmounts.get((long) 20);
            assertTrue(maxAmount > 5);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").value(20));
            executor.limit(limit);
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = (end - start);
            log.info("Age query took: {} with: {} results", passed, result.size());

            List<SimpleEntity> entities = aggregateResult(result);
            assertEquals("There should only be three results", limit, entities.size());
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
            log.info("Query execution took: {}", passed);

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
            log.info("Query execution took: {}", passed);

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
            log.info("Query execution took: {}", passed);

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
    public void testAndOperationMultiQueryBuilderTablescan() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("thosha");
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013d73bba1f7").addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013d73bba1f8").addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013dad2eefd2").addProperty("type", "contribution"));
        bag.addEntity(new SimpleEntity("00005442-4961-c49d-0000-013dd66f0aed").addProperty("type", "contribution"));

        try {
            QueryBuilder builder = QueryBuilder.createBuilder(BlockType.AND);
            builder.addQueryBlock(QueryBuilder.createBuilder().field("__ID").value("00005442-4961-c49d-0000-013dad2eefd2"));
            builder.addQueryBlock(QueryBuilder.createBuilder().field("type").value("contribution"));

            QueryExecutor executor = bag.find(builder);
            try (QueryResult result = executor.execute()) {
                assertThat(result.size(), is(1l));
            }
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
            log.info("Query execution took: {}", passed);

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
                                assertEquals((long) i, entity.getProperty("age").getFirstValueObject());
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
            log.info("Query execution took: {}", passed);

            assertNotNull(result);

            Assert.assertFalse("There should not be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }


    @Test
    public void testEqualsTablescan() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            /* we get the entity expected, by using the value on field5 which is the same ordering */
            String queryKey1 = "value50";
            String expectedId1 = valueToId.get(queryKey1);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field7").value("myValue50"));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            log.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId1, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsTablescanMultiFields() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            /* we get the entity expected, by using the value on field5 which is the same ordering */
            String queryKey1 = "value50";
            String expectedId1 = valueToId.get(queryKey1);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field7").value("myValue50").field("field8").value("myValue50"));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            log.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId1, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsPartialTablescanMultiFields() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            /* we get the entity expected, by using the value on field5 which is the same ordering */
            String queryKey1 = "value50";
            String expectedId1 = valueToId.get(queryKey1);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field7").value("myValue50").field("field1").value("value50"));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = end - start;
            log.info("Query execution took: {}", passed);

            assertNotNull(result);

            assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId1, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
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
            log.info("Average query time: {} for {} queries", (totalTime / queries), queries);
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

    @Test
    public void testEqualsMultivaluePagingRangeOperation() throws Exception {
        int batchSize = 100;
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("inverted");

        try {
            int start = 0;
            long current = 0;
            while(start + batchSize <= NUMBER_ENTITIES) {
                int end = start + batchSize;
                log.debug("Starting retrieval of start: {} and end: {}", start, end);

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field5").greaterThanOrEquals(0));
                executor.paging(start, batchSize);
                try (QueryResult result = executor.execute()) {
                    assertEquals("Unexpected query size", batchSize, result.size());
                    for (SimpleEntity entity : result) {
                        Property property = entity.getProperty("field5");
                        Property fProperty = entity.getProperty("field1");
                        assertEquals("Unexpected value", current, property.getFirstValueObject());
                        log.debug("Field1: {} Field5: {}", fProperty.getFirstValueObject().toString(), property.getFirstValueObject().toString());
                        current++;
                    }
                }

                start = end;
            }
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsMultivaluePagingRangeOperationTableScan() throws Exception {
        int batchSize = 100;
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("inverted");

        try {
            int start = 0;
            long current = 0;
            while(start + batchSize <= NUMBER_ENTITIES) {
                int end = start + batchSize;
                log.debug("Starting retrieval of start: {} and end: {}", start, end);

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field9").greaterThanOrEquals(0));
                executor.paging(start, batchSize);
                QueryResult result = executor.execute();
                assertEquals("Unexpected query size", batchSize, result.size());
                for(SimpleEntity entity : result) {
                    Property property = entity.getProperty("field9");
                    Property fProperty = entity.getProperty("field1");
                    assertEquals("Unexpected value", current, property.getFirstValueObject());
                    log.debug("Field1: {} Field5: {}", fProperty.getFirstValueObject().toString(), property.getFirstValueObject().toString());
                    current++;
                }

                start = end;
            }
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testMultivaluePagingEqualsOperation() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("inverted");

        try {
            int start = 0;
            long lage = (long) (MAX_AGE / 2);
            int ageSize = ageAmounts.get(lage);
            int batchSize = ageSize / 4;

            while(start + batchSize <= ageSize) {
                int end = start + batchSize;
                log.debug("Starting retrieval of start: {} and end: {} for age: {}", start, end, lage);

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").value(lage));
                executor.paging(start, batchSize);
                QueryResult result = executor.execute();
                assertEquals("Unexpected query size", batchSize, result.size());
                for(SimpleEntity entity : result) {
                    String age = entity.getProperty("age").getFirstValueObject().toString();
                    assertEquals("Unexpected age", String.valueOf(MAX_AGE / 2), age);
                }

                start = end;
            }
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }
    }

    private List<SimpleEntity> aggregateResult(QueryResult result) {
        List<SimpleEntity> entities = new ArrayList<>();

        for(SimpleEntity entity : result) {
            entities.add(entity);
        }

        return entities;
    }

    private List<String> assertResult(int start, int amount, QueryResult result) {
        List<String> keysFoundInOrder = new ArrayList<>();

        for(int i=start; i<(start + amount) && result.hasNext(); i++) {
            SimpleEntity entity = result.next();

            String expectedId = longToId.get((long) i);
            assertNotNull("There should be a returned entity", entity);
            assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field1");
            assertNotNull("Property should be set", property);
            assertTrue("Property should be String", property.getFirstValueObject() instanceof String);
            assertEquals("Property value should match", "value" + i, property.getFirstValueObject());
            keysFoundInOrder.add(entity.getInternalId());
        }

        return keysFoundInOrder;
    }

    @Test
    public void testRangeQuerySortByOtherFieldNaturalOrdering() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field5").greaterThan(10).field("field5").smallerThan(30).sortBy("field1"));
            try (QueryResult result = executor.execute()) {
                assertNotNull(result);
                assertTrue("There should be a result", result.hasNext());

                int start = 11;
                int amount = 18;
                List<String> keysInOrder = assertResult(start, amount, result);

                List<String> expectedOrder = new ArrayList<>();
                for (int i = start; i < amount + start; i++) {
                    String id = valueToId.get("value" + i);
                    expectedOrder.add(id);
                    log.info("Expected key: {} with value: {}", id, "value" + i);
                }
                assertListOrder(keysInOrder, expectedOrder);
            }
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testRangeQuerySortByOtherField() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");

        try {
            QueryBuilder query = QueryBuilder.createBuilder().field("field5").greaterThan(10).field("field5").smallerThan(30).sortBy("field6", Order.ASCENDING);
            List<SimpleEntity> entities = getEntities(bag, query);
            List<String> field6Values = getEntityValue(entities, "field6");

            long previous = 0;
            for(String stringValue : field6Values) {
                long value = Long.parseLong(stringValue);
                assertThat(value >= previous, is(true));
                previous = value;
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

//        List<SimpleEntity> plugins = getEntities(bag, PLUGIN_QUERY);
//        assertThat(plugins.size(), is(1));
//        assertThat(getEntityValue(plugins, SimpleEntity.DOCUMENT_ID), hasItems(pluginId));
//
//        List<SimpleEntity> controllers = getEntities(bag, CONTROLLER_QUERY);
//        assertThat(controllers.size(), is(1));
//        assertThat(getEntityValue(controllers, SimpleEntity.DOCUMENT_ID), hasItems(controllerId));

        List<SimpleEntity> devices = getEntities(bag, DEVICE_QUERY);
        assertThat(devices.size(), is(1));
        assertThat(getEntityValue(devices, SimpleEntity.DOCUMENT_ID), hasItems(deviceId));
    }

    private List<String> getEntityValue(List<SimpleEntity> entities, final String property) {
        return Lists.transform(entities, new Function<SimpleEntity, String>() {
            @Override
            public String apply(SimpleEntity entity) {
                return entity.getProperty(property).getFirstValue().toString();
            }
        });
    }

    private List<SimpleEntity> getEntities(EntityBag bag, QueryBuilder query) throws JasDBStorageException {
        return getEntities(bag, query, -1, -1);
    }

    private List<SimpleEntity> getEntities(EntityBag bag, QueryBuilder query, int start, int limit) throws JasDBStorageException {
        QueryExecutor executor = bag.find(query);

        if(start > 0 && limit > 0) {
            executor.paging(start, limit);
        } else if(limit > 0) {
            executor.limit(limit);
        }

        final List<SimpleEntity> entities = new ArrayList<>();
        try (QueryResult result = executor.execute()) {
            for (SimpleEntity entity : result) {
                entities.add(entity);
            }
        }

        return entities;
    }

    private void assertListOrder(List<String> expectedOrder, List<String> actualOrder) {
        int counter = 0;
        for(String expectedId : expectedOrder) {
            log.info("Key found in order: {}", actualOrder.get(counter));
            String actualId = actualOrder.get(counter);
            assertEquals("The id's of index: " + counter + " are not expected in that order", expectedId, actualId);

            counter++;
        }
    }

}
