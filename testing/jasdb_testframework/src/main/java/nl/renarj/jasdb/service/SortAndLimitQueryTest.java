package nl.renarj.jasdb.service;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public abstract class SortAndLimitQueryTest extends QueryBaseTest {
    private static final Logger LOG = getLogger(SortAndLimitQueryTest.class);

    public SortAndLimitQueryTest(DBSessionFactory sessionFactory) {
        super(sessionFactory);
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
            innerQuery.field("name").value("xxx").sortBy("v", Order.DESCENDING);

            QueryExecutor executor = bag.find(innerQuery);
            QueryResult result = executor.execute();
            assertThat(result.size(), is(2l));

            assertThat(result.next().getValue("v"), is("3"));
            assertThat(result.next().getValue("v"), is("2"));
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
            innerQuery.field("name").value("xxx").sortBy("_id",Order.DESCENDING).sortBy("id", Order.DESCENDING);

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
            LOG.info("Age query took: {} with: {} results", passed, result.size());

            List<SimpleEntity> entities = aggregateResult(result);
            assertEquals("There should only be three results", limit, entities.size());
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
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
                    LOG.info("Expected key: {} with value: {}", id, "value" + i);
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
                LOG.debug("Starting retrieval of start: {} and end: {}", start, end);

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field5").greaterThanOrEquals(0));
                executor.paging(start, batchSize);
                try (QueryResult result = executor.execute()) {
                    assertEquals("Unexpected query size", batchSize, result.size());
                    for (SimpleEntity entity : result) {
                        Property property = entity.getProperty("field5");
                        Property fProperty = entity.getProperty("field1");
                        assertEquals("Unexpected value", current, (long) property.getFirstValueObject());
                        LOG.debug("Field1: {} Field5: {}", fProperty.getFirstValueObject().toString(), property.getFirstValueObject().toString());
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
                LOG.debug("Starting retrieval of start: {} and end: {}", start, end);

                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("field9").greaterThanOrEquals(0));
                executor.paging(start, batchSize);
                QueryResult result = executor.execute();
                assertEquals("Unexpected query size", batchSize, result.size());
                for(SimpleEntity entity : result) {
                    Property property = entity.getProperty("field9");
                    Property fProperty = entity.getProperty("field1");
                    assertEquals("Unexpected value", current, (long) property.getFirstValueObject());
                    LOG.debug("Field1: {} Field5: {}", fProperty.getFirstValueObject().toString(), property.getFirstValueObject().toString());
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
                LOG.debug("Starting retrieval of start: {} and end: {} for age: {}", start, end, lage);

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


    private void assertListOrder(List<String> expectedOrder, List<String> actualOrder) {
        int counter = 0;
        for(String expectedId : expectedOrder) {
            LOG.info("Key found in order: {}", actualOrder.get(counter));
            String actualId = actualOrder.get(counter);
            assertEquals("The id's of index: " + counter + " are not expected in that order", expectedId, actualId);

            counter++;
        }
    }

    private List<SimpleEntity> aggregateResult(QueryResult result) {
        List<SimpleEntity> entities = new ArrayList<>();

        for(SimpleEntity entity : result) {
            entities.add(entity);
        }

        return entities;
    }
}
