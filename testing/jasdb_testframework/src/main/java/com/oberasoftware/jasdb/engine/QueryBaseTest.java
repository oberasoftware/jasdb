package com.oberasoftware.jasdb.engine;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.SimpleBaseTest;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.DBSessionFactory;
import nl.renarj.jasdb.api.EmbeddedEntity;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.api.properties.Property;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public abstract class QueryBaseTest {
    protected static final int NUMBER_ENTITIES = 1000;
    protected static final int MAX_AGE = 50;

    protected Map<Long, String> longToId = new HashMap<>();
    protected Map<String, String> valueToId = new HashMap<>();
    protected Map<Long, Integer> ageAmounts = new HashMap<>();
    protected Map<String, Integer> cityCounters = new HashMap<>();


    protected DBSessionFactory sessionFactory;

    protected QueryBaseTest(DBSessionFactory sessionFactory) {
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
        bag.ensureIndex(new IndexField("city", new StringKeyType()), false);
        bag.ensureIndex(new IndexField("embed.embeddedProperty", new StringKeyType()), false);
        bag.ensureIndex(new CompositeIndexField(new IndexField("age", new LongKeyType()), new IndexField("mainCity", new StringKeyType())), false);

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

    protected List<SimpleEntity> toList(QueryResult result) {
        List<SimpleEntity> entities = new ArrayList<>();
        result.forEach(entities::add);
        return entities;
    }

    protected List<String> assertResult(int start, int amount, QueryResult result) {
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

    protected List<String> getEntityValue(List<SimpleEntity> entities, final String property) {
        return Lists.transform(entities, entity -> entity.getProperty(property).getFirstValue().toString());
    }

    protected List<SimpleEntity> getEntities(EntityBag bag, QueryBuilder query) throws JasDBStorageException {
        return getEntities(bag, query, -1, -1);
    }

    protected List<SimpleEntity> getEntities(EntityBag bag, QueryBuilder query, int start, int limit) throws JasDBStorageException {
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

}
