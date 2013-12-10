/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service;

import junit.framework.Assert;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.BlockType;
import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        bag.ensureIndex(new CompositeIndexField(new IndexField("age", new LongKeyType()), new IndexField("mainCity", new StringKeyType(200))), false);

        Random rnd = new Random(System.currentTimeMillis());
        for(int i=0; i< NUMBER_ENTITIES; i++) {
            String value = "value" + i;
            Long lValue = Long.valueOf(i);
            String fieldValue = "myValue" + i;
            Long age = Long.valueOf(rnd.nextInt(MAX_AGE));

            String city1 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];

            String city2 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];
            while(city2.equals(city1)) {
                city2 = SimpleBaseTest.possibleCities[rnd.nextInt(SimpleBaseTest.possibleCities.length)];
            }

            SimpleEntity entity = bag.addEntity(new SimpleEntity()
                    .addProperty("field1", value)
                    .addProperty("field5", lValue)
                    .addProperty("field6", Long.valueOf(NUMBER_ENTITIES - i))
                    .addProperty("field7", fieldValue)
                    .addProperty("field8", fieldValue)
                    .addProperty("field9", lValue)
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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field5");
            Assert.assertNotNull("Property should be set", property);
            Assert.assertTrue("Property should be long", property.getFirstValueObject() instanceof Long);

            String longExpectedId = longToId.get(property.getFirstValueObject());
            Assert.assertEquals("The id's should match", longExpectedId, entity.getInternalId());

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
                QueryResult result = executor.execute();
                long end = System.nanoTime();
                long passed = end - start;
                log.info("Query execution took: {}", passed);

                try {
                    int expected = NUMBER_ENTITIES - ageAmounts.get(Long.valueOf(age));

                    for(SimpleEntity entity : result) {
                        assertThat(entity.getValue("age").toString(), not(equalTo(String.valueOf(age))));
                    }
                    assertThat(result.size(), is((long)expected));
                } finally {
                    result.close();
                }
            }

        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testEqualsAgeWithLimiting() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        try {
            int limit = 3;
            Integer maxAmount = ageAmounts.get(Long.valueOf(20));
            Assert.assertTrue(maxAmount > 5);

            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").value(20));
            executor.limit(limit);
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            long passed = (end - start);
            log.info("Age query took: {} with: {} results", passed, result.size());

            List<SimpleEntity> entities = aggregateResult(result);
            Assert.assertEquals("There should only be three results", limit, entities.size());
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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field5");
            Assert.assertNotNull("Property should be set", property);
            Assert.assertTrue("Property should be long", property.getFirstValueObject() instanceof Long);

            String longExpectedId = longToId.get(property.getFirstValueObject());
            Assert.assertEquals("The id's should match", longExpectedId, entity.getInternalId());

            Assert.assertFalse("There should no longer be a result", result.hasNext());
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
            QueryResult result = executor.execute();
            try {
                assertThat(result.size(), is(1l));
            } finally {
                result.close();
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
            QueryResult result = executor.execute();
            try {
                assertThat(result.size(), is(0l));
            } finally {
                result.close();
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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity1 = result.next();
            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity2 = result.next();

            Assert.assertNotNull("There should be a returned entity", entity1);
            Assert.assertEquals("The id's should match", expectedId1, entity1.getInternalId());

            Assert.assertNotNull("There should be a returned entity", entity2);
            Assert.assertEquals("The id's should match", expectedId2, entity2.getInternalId());
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
                    QueryResult result = executor.execute();
                    try {
                        String key = city + "_" + i;
                        if(cityCounters.containsKey(key)) {
                            Long counter = new Long(cityCounters.get(city + "_" + i));
                            assertEquals(counter, new Long(result.size()));

                            for(SimpleEntity entity : result) {
                                assertEquals(Long.valueOf(i), entity.getProperty("age").getFirstValueObject());
                                assertEquals(city, entity.getProperty("mainCity").getFirstValueObject());
                            }
                        } else {
                            assertEquals(new Long(0), new Long(result.size()));
                        }
                    } finally {
                        result.close();
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

            Assert.assertNotNull(result);

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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId1, entity.getInternalId());

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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId1, entity.getInternalId());

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

            Assert.assertNotNull(result);

            Assert.assertTrue("There should be a result", result.hasNext());
            SimpleEntity entity = result.next();
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId1, entity.getInternalId());

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
            QueryResult result = executor.execute();
            try {
                Assert.assertEquals(19, result.size());
                Assert.assertNotNull(result);

                Assert.assertTrue("There should be a result", result.hasNext());
                assertResult(11, 18, result);
            } finally {
                result.close();
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
                QueryResult result = executor.execute();
                try {
                    Assert.assertNotNull(result);
                    Assert.assertEquals("Results for city: '" + city + "' are unexpected", new Long(cityCounters.get(city)), new Long(result.size()));
                } finally {
                    result.close();
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
                        QueryResult result = executor.execute();
                        long end = System.nanoTime();
                        totalTime += (end - start);
                        queries++;

                        try {
                            Assert.assertNotNull(result);
                            Assert.assertEquals("Results for combined cities: '" + firstCity + ", " + secondCity + "' are unexpected",
                                    new Long(cityCounters.get(firstCity + "_" + secondCity)), new Long(result.size()));
                        } finally {
                            result.close();
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
                QueryResult result = executor.execute();
                try {
                    Assert.assertFalse("There should be no result", result.hasNext());
                } finally {
                    result.close();
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
                QueryResult result = executor.execute();
                try {
                    Assert.assertEquals("Unexpected query size", batchSize, result.size());
                    for(SimpleEntity entity : result) {
                        Property property = entity.getProperty("field5");
                        Property fProperty = entity.getProperty("field1");
                        Assert.assertEquals("Unexpected value", Long.valueOf(current), property.getFirstValueObject());
                        log.debug("Field1: {} Field5: {}", fProperty.getFirstValueObject().toString(), property.getFirstValueObject().toString());
                        current++;
                    }
                } finally {
                    result.close();
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
                Assert.assertEquals("Unexpected query size", batchSize, result.size());
                for(SimpleEntity entity : result) {
                    Property property = entity.getProperty("field9");
                    Property fProperty = entity.getProperty("field1");
                    Assert.assertEquals("Unexpected value", Long.valueOf(current), property.getFirstValueObject());
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
            long lage = Long.valueOf(MAX_AGE / 2);
            int ageSize = ageAmounts.get(lage);
            int batchSize = ageSize / 4;

            while(start + batchSize <= ageSize) {
                int end = start + batchSize;
                log.debug("Starting retrieval of start: {} and end: {} for age: {}", new Object[] {start, end, lage});

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("age").value(lage));
                executor.paging(start, batchSize);
                QueryResult result = executor.execute();
                Assert.assertEquals("Unexpected query size", batchSize, result.size());
                for(SimpleEntity entity : result) {
                    String age = entity.getProperty("age").getFirstValueObject().toString();
                    Assert.assertEquals("Unexpected age", String.valueOf(MAX_AGE / 2), age);
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

            String expectedId = longToId.get(Long.valueOf(i));
            Assert.assertNotNull("There should be a returned entity", entity);
            Assert.assertEquals("The id's should match", expectedId, entity.getInternalId());

            Property property = entity.getProperty("field1");
            Assert.assertNotNull("Property should be set", property);
            Assert.assertTrue("Property should be String", property.getFirstValueObject() instanceof String);
            Assert.assertEquals("Property value should match", "value" + i, property.getFirstValueObject());
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
            QueryResult result = executor.execute();
            try {
                Assert.assertNotNull(result);
                Assert.assertTrue("There should be a result", result.hasNext());

                int start = 11;
                int amount = 18;
                List<String> keysInOrder = assertResult(start, amount, result);

                List<String> expectedOrder = new ArrayList<>();
                for(int i=start; i<amount + start; i++) {
                    String id = valueToId.get("value" + i);
                    expectedOrder.add(id);
                    log.info("Expected key: {} with value: {}", id, "value" + i);
                }
                assertListOrder(keysInOrder, expectedOrder);
            } finally {
                result.close();
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
            QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field5").greaterThan(10).field("field5").smallerThan(30).sortBy("field6", Order.ASCENDING));
            long start = System.nanoTime();
            QueryResult result = executor.execute();
            long end = System.nanoTime();
            log.info("Query took: {}", (end - start));

            StatisticsMonitor.enableStatistics();
            start = System.nanoTime();
            result = executor.execute();
            end = System.nanoTime();
            log.info("Query took: {}", (end - start));
            Thread.sleep(1000);
            StatisticsMonitor.logStats(TimeUnit.NANOSECONDS);

            try {
                Assert.assertNotNull(result);
                Assert.assertTrue("There should be a result", result.hasNext());

                for(SimpleEntity entity : result) {
                    log.info("Found entity: {} with value: {}", entity.getInternalId(), entity.getProperty("field6").getFirstValueObject());
                }
            } finally {
                result.close();
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
            QueryResult result = executor.execute();

            try {
                Assert.assertNotNull(result);
                Assert.assertFalse("There should not be a result", result.hasNext());
            } finally {
                result.close();
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

    private void assertListOrder(List<String> expectedOrder, List<String> actualOrder) {
        int counter = 0;
        for(String expectedId : expectedOrder) {
            log.info("Key found in order: {}", actualOrder.get(counter));
            String actualId = actualOrder.get(counter);
            Assert.assertEquals("The id's of index: " + counter + " are not expected in that order", expectedId, actualId);

            counter++;
        }
    }

}
