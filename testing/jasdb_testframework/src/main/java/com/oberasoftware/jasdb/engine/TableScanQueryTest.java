package com.oberasoftware.jasdb.engine;

import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.query.BlockType;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
public abstract class TableScanQueryTest extends QueryBaseTest {
    private static final Logger LOG = getLogger(TableScanQueryTest.class);
    private static final String ID1 = "00005442-4961-c49d-0000-013d73bba1f7";
    private static final String ID2 = "00005442-4961-c49d-0000-013d73bba1f8";
    private static final String ID3 = "00005442-4961-c49d-0000-013dad2eefd2";
    private static final String ID4 = "00005442-4961-c49d-0000-013dd66f0aed";

    public TableScanQueryTest(DBSessionFactory sessionFactory) {
        super(sessionFactory);
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
            LOG.info("Query execution took: {}", passed);

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
            LOG.info("Query execution took: {}", passed);

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
            LOG.info("Query execution took: {}", passed);

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
    public void testAndOperationMultiQueryBuilderTablescan() throws Exception {
        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("thosha");
        bag.addEntity(new SimpleEntity(ID1).addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity(ID2).addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity(ID3).addProperty("type", "contribution"));
        bag.addEntity(new SimpleEntity(ID4).addProperty("type", "contribution"));

        try {
            QueryBuilder builder = QueryBuilder.createBuilder(BlockType.AND);
            builder.addQueryBlock(QueryBuilder.createBuilder().field("__ID").value(ID3));
            builder.addQueryBlock(QueryBuilder.createBuilder().field("type").value("contribution"));

            QueryExecutor executor = bag.find(builder);
            List<SimpleEntity> entities = toList(executor.execute());
            assertThat(entities.size(), is(1));
        } finally {
            pojoDb.closeSession();
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testNoConditionsFullScan() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("smallbag");
        bag.addEntity(new SimpleEntity(ID1).addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity(ID2).addProperty("type", "thing"));
        bag.addEntity(new SimpleEntity(ID3).addProperty("type", "contribution"));
        bag.addEntity(new SimpleEntity(ID4).addProperty("type", "contribution"));

        try {
            QueryBuilder builder = QueryBuilder.createBuilder();

            QueryExecutor executor = bag.find(builder);
            List<SimpleEntity> entities = toList(executor.execute());
            List<String> entityIds = entities.stream().map(SimpleEntity::getInternalId).collect(Collectors.toList());

            assertThat(entityIds, hasItems(ID1, ID2, ID3, ID4));
        } finally {
            session.closeSession();
            SimpleKernel.shutdown();
        }

    }
}
