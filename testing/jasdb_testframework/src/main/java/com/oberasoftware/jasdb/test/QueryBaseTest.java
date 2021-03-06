package com.oberasoftware.jasdb.test;

import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.session.*;
import com.oberasoftware.jasdb.core.index.query.SimpleCompositeIndexField;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.core.EmbeddedEntity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;
import com.oberasoftware.jasdb.api.session.query.QueryExecutor;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Renze de Vries
 */
public abstract class QueryBaseTest {
    static final int NUMBER_ENTITIES = 1000;
    static final int MAX_AGE = 50;

    Map<Long, String> longToId = new HashMap<>();
    Map<String, String> valueToId = new HashMap<>();
    Map<Long, Integer> ageAmounts = new HashMap<>();
    Map<String, Integer> cityCounters = new HashMap<>();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    DBSessionFactory sessionFactory;

    QueryBaseTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @After
    public void tearDown() throws Exception {
        JasDBMain.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, temporaryFolder.newFolder().toString());
        JasDBMain.start();

        DBSession pojoDb = sessionFactory.createSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        bag.ensureIndex(new SimpleIndexField("field1", new StringKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("field5", new LongKeyType()), false, new SimpleIndexField("field6", new LongKeyType()));
        bag.ensureIndex(new SimpleIndexField("field6", new LongKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("age", new LongKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("city", new StringKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("embed.embeddedProperty", new StringKeyType()), false);
        bag.ensureIndex(new SimpleCompositeIndexField(new SimpleIndexField("age", new LongKeyType()), new SimpleIndexField("mainCity", new StringKeyType())), false);

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

            Entity entity = bag.addEntity(new SimpleEntity()
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

    List<Entity> toList(QueryResult result) {
        List<Entity> entities = new ArrayList<>();
        result.forEach(entities::add);
        return entities;
    }

    List<String> assertResult(int start, int amount, QueryResult result) {
        List<String> keysFoundInOrder = new ArrayList<>();

        for(int i=start; i<(start + amount) && result.hasNext(); i++) {
            Entity entity = result.next();

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

    List<String> getEntityValue(List<Entity> entities, final String property) {
        return Lists.transform(entities, entity -> {
            assert entity != null;
            return entity.getProperty(property).getFirstValue().toString();
        });
    }

    List<Entity> getEntities(EntityBag bag, QueryBuilder query) throws JasDBStorageException {
        return getEntities(bag, query, -1, -1);
    }

    private List<Entity> getEntities(EntityBag bag, QueryBuilder query, int start, int limit) throws JasDBStorageException {
        QueryExecutor executor = bag.find(query);

        if(start > 0 && limit > 0) {
            executor.paging(start, limit);
        } else if(limit > 0) {
            executor.limit(limit);
        }

        final List<Entity> entities = new ArrayList<>();
        try (QueryResult result = executor.execute()) {
            for (Entity entity : result) {
                entities.add(entity);
            }
        }

        return entities;
    }

}
