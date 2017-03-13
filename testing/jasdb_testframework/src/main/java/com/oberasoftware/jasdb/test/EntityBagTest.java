package com.oberasoftware.jasdb.test;

import com.oberasoftware.jasdb.api.session.*;
import com.oberasoftware.jasdb.core.index.query.SimpleCompositeIndexField;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.engine.HomeLocatorUtil;
import com.oberasoftware.jasdb.service.JasDBMain;
import com.oberasoftware.jasdb.core.utils.ResourceUtil;
import com.oberasoftware.jasdb.core.EmbeddedEntity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;
import com.oberasoftware.jasdb.api.session.query.QueryExecutor;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.core.caching.GlobalCachingMemoryManager;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public abstract class EntityBagTest {
	private Logger log = LoggerFactory.getLogger(EntityBagTest.class);
	
	private static final int INITIAL_SIZE = 100;
	private static final int NUMBER_ENTITIES = 10000;
	private static final String searchTestId = "f5533a4a-14e2-42fc-94db-bac3fc0b1712";

    private static final String[] cities = new String[] {"Amsterdam", "Rotterdam", "Utrecht", "Groningen", "Haarlem",
            "Den Haag", "Maastricht", "Eindhoven"};
    
    private DBSessionFactory sessionFactory;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String storageLocation;
	
	@After
	public void tearDown() throws Exception {
        JasDBMain.shutdown();
    }
	
	@Before
	public void setUp() throws Exception {
	    storageLocation = temporaryFolder.newFolder().toString();
        System.setProperty(HomeLocatorUtil.JASDB_HOME, storageLocation);
        JasDBMain.start();
	}

    protected EntityBagTest(DBSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
	
	@Test
	public void testPersist() throws Exception {
		String randomId = "";
		DBSession pojoDb = sessionFactory.createSession();
		EntityBag bag = pojoDb.createOrGetBag("testbag");
        bag.ensureIndex(new SimpleIndexField("title", new StringKeyType()), true);
		
		try {
            log.info("Starting insert of: {}", NUMBER_ENTITIES);
            Random rnd = new Random(System.currentTimeMillis());
            long start = System.currentTimeMillis();
            for(int i=0; i<NUMBER_ENTITIES; i++) {
				SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
                entity.addProperty("title", "title" + i);
				for(int j=0; j<10; j++) {
					entity.addProperty("test-" + i + "-" + j, i + "-" + j);
					entity.addProperty("testString-" + i + "-" + j, "String-" + i + "-" + j);
					
					String randomString = "";
                    int r = rnd.nextInt(50);
					for(int k=0; k<r; k++) {
						randomString += 'x';
					}
					entity.addProperty("randomString-" + i + "-" + j, randomString);
				}
				bag.addEntity(entity);

				if(i == (NUMBER_ENTITIES / 2)) {
					randomId = entity.getInternalId();
				}

                if(i % 100000 == 0) {
                    log.info("Inserted: {} in: {}", i, (System.currentTimeMillis() - start));
                    log.info("Current memory size: {}", GlobalCachingMemoryManager.getGlobalInstance().calculateMemorySize());
                }
			}
            long end = System.currentTimeMillis();
            log.info("Finished insert in: {}", TimeUnit.SECONDS.convert((end - start), TimeUnit.MILLISECONDS));
			SimpleEntity searchEntity = new SimpleEntity(UUID.randomUUID().toString());
			searchEntity.addProperty("MySearchProperty", "MySpecialSearchProperty");
			searchEntity.setInternalId(searchTestId);
			bag.addEntity(searchEntity);
		} finally {
            JasDBMain.shutdown();
		}

		assertFind(randomId);
		testReadBag();
	}
	
	private void testReadBag() throws Exception {
        JasDBMain.start();
		DBSession pojoDb = sessionFactory.createSession();
		EntityBag bag = pojoDb.createOrGetBag("testbag");

		try {
			long startTime = System.currentTimeMillis();
			QueryResult result = bag.getEntities();
			int foundEntities = getResultSize(result);
			
			long endTime = System.currentTimeMillis();
			log.info("Took: {} ms. to read: {} entities from 'testbag'", (endTime - startTime), foundEntities);
			assertEquals("Unexpected number of entities found", NUMBER_ENTITIES + 1, foundEntities);
			
			result = bag.getEntities(20);
			foundEntities = getResultSize(result);
			assertEquals("Unexpected number of entities found", 20, foundEntities);
		} finally {
			JasDBMain.shutdown();
		}
	}
	
	private int getResultSize(QueryResult result) {
		int foundEntities = 0;
		for(Entity entity : result) {
			foundEntities++;
			log.debug("Iterating entity: {} found: {}", entity.getInternalId(), foundEntities);
		}

		return foundEntities;
	}
	
	private void assertFind(String someRandomId) throws Exception {
        JasDBMain.start();
		log.info("START INDEX READ TEST");
		DBSession pojoDb = sessionFactory.createSession();
		EntityBag bag = pojoDb.createOrGetBag("testbag");

		try {
			log.info("Starting search for: {}", searchTestId);
			long startSearch = System.nanoTime();
            Entity entity = bag.getEntity(searchTestId);
			long endSearch = System.nanoTime();
			log.info("Search finished in: {}", (endSearch - startSearch));

			Assert.assertNotNull("An entity should have been found for id: " + searchTestId, entity);
			assertEquals(searchTestId, entity.getInternalId());
			
			log.info("Starting search for random: {}", someRandomId);
			startSearch = System.nanoTime();
			entity = bag.getEntity(someRandomId);
			endSearch = System.nanoTime();
			log.info("Search finished for random: {}", (endSearch - startSearch));

			Assert.assertNotNull("An entity should have been found", entity);
			assertEquals(someRandomId, entity.getInternalId());
		} finally {
			JasDBMain.shutdown();
		}
	}

    @Test
    public void testInvalidJsonInsert() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("MySpecialBag");

        SimpleEntity entity = new SimpleEntity();
        entity.addProperty("title", "Title of my content");
        entity.addProperty("text", "Some big piece of text content");
        bag.addEntity(entity);

        QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("text").value("Some big piece of text content"));
        QueryResult result = executor.execute();

        assertThat(result.size(), is(1L));
        for(Entity resultEntity : result) {
            String json = SimpleEntity.toJson(resultEntity);
            log.info("Output: {}", json);
        }
    }

    @Test
    public void testNotEqualsMultiIndexes() throws Exception {
        try {
            DBSession session = sessionFactory.createSession();
            EntityBag bag = session.createOrGetBag("websites");
            bag.ensureIndex(new SimpleIndexField("url", new StringKeyType()), false);
            bag.ensureIndex(
                    new SimpleCompositeIndexField(
                            new SimpleIndexField("stepid", new StringKeyType()),
                            new SimpleIndexField("workflow", new LongKeyType())
                    ),false);

            bag.addEntity(new SimpleEntity().addProperty("url", "").addProperty("stepid", 1L).addProperty("workflow", 1L));
            bag.addEntity(new SimpleEntity().addProperty("url", "").addProperty("stepid", 1L).addProperty("workflow", 1L));
            bag.addEntity(new SimpleEntity().addProperty("url", "").addProperty("stepid", 1L).addProperty("workflow", 1L));

            bag.addEntity(new SimpleEntity().addProperty("url", "http://someurl.nl/1").addProperty("stepid", 1L).addProperty("workflow", 1L));
            bag.addEntity(new SimpleEntity().addProperty("url", "http://someurl.nl/2").addProperty("stepid", 1L).addProperty("workflow", 1L));

            QueryResult r = bag.find(QueryBuilder.createBuilder()
                    .field("stepid").value(1L)
                    .field("workflow").value(1L)
                    .field("url").notEquals("")).execute();
            assertThat(r.size(), is(2L));
        } finally {
            JasDBMain.shutdown();
        }
    }

    @Test
    public void testDeleteEmptyIndexValue() throws Exception {
        try {
            DBSession session = sessionFactory.createSession();
            EntityBag bag = session.createOrGetBag("somebag");
            bag.ensureIndex(new SimpleIndexField("field", new StringKeyType()), false);

            String id = bag.addEntity(new SimpleEntity().addProperty("anotherfield", "somevalue")).getInternalId();

            bag.removeEntity(id);
        } finally {
            JasDBMain.shutdown();
        }
    }

    @Test
    public void testPersistUpdateShutdownRead() throws Exception {
        Entity entity1 = null, entity2 = null;
        try {
            DBSession pojoDb = sessionFactory.createSession();
            EntityBag bag = pojoDb.createOrGetBag("mybag");

            entity1 = bag.addEntity(new SimpleEntity());
            entity2 = bag.addEntity(new SimpleEntity());
        } finally {
            JasDBMain.shutdown();
        }
        String entity1Id = entity1.getInternalId();
        String entity2Id = entity2.getInternalId();

        JasDBMain.start();
        try {
            DBSession pojoDb = sessionFactory.createSession();
            EntityBag bag = pojoDb.createOrGetBag("mybag");


            entity1 = bag.getEntity(entity1.getInternalId());
            assertNotNull(entity1);
            assertEquals(entity1Id, entity1.getInternalId());
            entity1.addProperty("testProperty", "My value for entity 1");
            bag.updateEntity(entity1);

            entity2 = bag.getEntity(entity2.getInternalId());
            assertNotNull(entity2);
            assertEquals(entity2Id, entity2.getInternalId());
            entity2.addProperty("someProp", "Value 1 smaller");
            bag.updateEntity(entity2);

        } finally {
            JasDBMain.shutdown();
        }

        JasDBMain.start();
        try {
            DBSession pojoDb = sessionFactory.createSession();
            EntityBag bag = pojoDb.createOrGetBag("mybag");

            entity1 = bag.getEntity(entity1Id);
            assertNotNull(entity1);
            assertEquals(entity1Id, entity1.getInternalId());
            assertEquals("My value for entity 1", entity1.getProperty("testProperty").getFirstValueObject());

            entity2 = bag.getEntity(entity2Id);
            assertNotNull(entity2);
            assertEquals(entity2Id, entity2.getInternalId());
            assertEquals("Value 1 smaller", entity2.getProperty("someProp").getFirstValueObject());
        } finally {
            JasDBMain.shutdown();
        }
    }
	
	@Test
	public void testPersistFindPerformance() throws Exception {
		DBSession pojoDb = sessionFactory.createSession();
		EntityBag bag = pojoDb.createOrGetBag("mybag");

		List<String> entityIds = new ArrayList<>();
		for(int i=0; i<NUMBER_ENTITIES; i++) {
			SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
			entity.addProperty("someProperty" + i, i);
			entity.addProperty("doubleId", entity.getInternalId());
			bag.addEntity(entity);
			entityIds.add(entity.getInternalId());
		}

		try {
			for(String id : entityIds) {
                Entity entity = bag.getEntity(id);
				Assert.assertNotNull("Entity for id: " + id + " should be found", entity);
				assertEquals("Id should match expected id", id, entity.getInternalId());
				Assert.assertNotNull("There should be a property doubleId", entity.getProperty("doubleId"));
				assertEquals("Property doubleId should match expected id", id, entity.getProperty("doubleId").getFirstValueObject());
			}
		} finally {
            JasDBMain.shutdown();
		}
	}

    @Test
    public void testPersistMultiValue() throws Exception {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");

        try {
            Entity entity = new SimpleEntity();
            entity.addProperty("field1", "value1");
            entity.addProperty("field1", "value2");
            entity.addProperty("field1", "value3");
            entity.addProperty("number", 100L);
            entity.addProperty("number", 500L);
            bag.addEntity(entity);

            String entityId = entity.getInternalId();

            entity = bag.getEntity(entityId);
            Property property = entity.getProperty("field1");
            Assert.assertNotNull(property);
            assertEquals("The object should be multivalue", true, property.isMultiValue());
            assertEquals("There should be three properties", 3, property.getValues().size());
            assertEquals("Unexpected value", "value1", property.getValues().get(0).getValue());
            assertEquals("Unexpected value", "value2", property.getValues().get(1).getValue());
            assertEquals("Unexpected value", "value3", property.getValues().get(2).getValue());
            property = entity.getProperty("number");
            assertEquals("The object should be multivalue", true, property.isMultiValue());
            assertEquals("There should be three properties", 2, property.getValues().size());
            assertEquals("Unexpected value", 100L, property.getValues().get(0).getValue());
            assertEquals("Unexpected value", 500L, property.getValues().get(1).getValue());
        } finally {
            JasDBMain.shutdown();
        }
    }

    @Test
    public void testRandomPersistUpdate() throws Exception {
        String[] cities = new String[] {"Amsterdam", "Rotterdam", "Utrecht", "Groningen", "Haarlem", "Den Haag", "Maastricht", "Eindhoven"};
        int testSize = 1000;
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");
        bag.ensureIndex(new SimpleIndexField("city", new StringKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("itemId", new LongKeyType()), true);
        try {
            Map<String, Integer> cityCounts = generateCities(testSize, bag);
            assertCityIndexes(bag, cities, cityCounts);

            for(int i=200; i<400; i++) {
                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("itemId").value((long)i));
                QueryResult result = executor.execute();
                for(Entity entity : result) {
                    String city = entity.getProperty("city").getFirstValueObject().toString();
                    entity.setProperty("city", "unknown");
                    bag.updateEntity(entity);

                    changeCityCount(city, cityCounts, false);
                    changeCityCount("unknown", cityCounts, true);
                }
            }
            assertCityIndexes(bag, cities, cityCounts);
        } finally {
            JasDBMain.shutdown();
        }
    }

    @Test
    public void testRandomPersistUpdateBigRecordUpdate() throws Exception {
        String htmlData = ResourceUtil.getContent("datasets/htmlpage.data", "UTF-8");

        int testSize = 1000;
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");
        bag.ensureIndex(new SimpleIndexField("city", new StringKeyType()), false);
        bag.ensureIndex(new SimpleIndexField("itemId", new LongKeyType()), true);
        try {

            Map<String, Integer> cityCounts = generateCities(testSize, bag);
            assertCityIndexes(bag, cities, cityCounts);

            for(int i=200; i<400; i++) {
                QueryExecutor executor = bag.find(QueryBuilder.createBuilder().field("itemId").value((long)i));
                QueryResult result = executor.execute();
                for(Entity entity : result) {
                    String city = entity.getProperty("city").getFirstValueObject().toString();
                    entity.setProperty("city", "unknown");
                    entity.setProperty("bigdatafield", htmlData);
                    bag.updateEntity(entity);

                    changeCityCount(city, cityCounts, false);
                    changeCityCount("unknown", cityCounts, true);
                }
            }
            assertCityIndexes(bag, cities, cityCounts);
        } finally {
            JasDBMain.shutdown();
        }
    }

    private Map<String, Integer> generateCities(int testSize, EntityBag bag) throws JasDBStorageException {
        Map<String, Integer> cityCounts = new HashMap<>();
        Random rnd = new Random();
        for(int i=0; i<testSize; i++) {
            int cityIdx = rnd.nextInt(cities.length);
            String city = cities[cityIdx];
            SimpleEntity entity = new SimpleEntity();
            entity.addProperty("city", city);
            entity.addProperty("itemId", (long)i);

            changeCityCount(city, cityCounts, true);
            bag.addEntity(entity);
        }
        return cityCounts;
    }

    @Test
    public void testPersisterRemove() throws Exception {
        int testSize = 1000;
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");
        bag.ensureIndex(new SimpleIndexField("city", new StringKeyType(100)), false);
        bag.ensureIndex(new SimpleIndexField("testField", new LongKeyType()), true);

        Random rnd = new Random();
        for(int i=0; i<testSize; i++) {
            int cityIdx = rnd.nextInt(SimpleBaseTest.possibleCities.length);
            String city = SimpleBaseTest.possibleCities[cityIdx];

            SimpleEntity entity = new SimpleEntity();
            entity.addProperty("city", city);
            entity.addProperty("testField", (long)i);
            bag.addEntity(entity);
        }

        for(String city : SimpleBaseTest.possibleCities) {
            QueryResult result = bag.find(QueryBuilder.createBuilder().field("city").value(city)).execute();
            for(Entity foundEntity : result) {
                Long testFieldValue = foundEntity.getProperty("testField").getFirstValueObject();
                bag.removeEntity(foundEntity);

                assertFalse("There should no longer be a result", bag.find(QueryBuilder.createBuilder().field("testField").value(testFieldValue)).execute().hasNext());
            }

            result = bag.find(QueryBuilder.createBuilder().field("city").value(city)).execute();
            assertEquals("There should no longer be any entity", (long) 0, result.size());
        }
    }
    
    private void assertCityIndexes(EntityBag bag, String[] cities, Map<String, Integer> expectedCounts) throws Exception {
        for(String city : cities) {
            QueryResult r = bag.find(QueryBuilder.createBuilder().field("city").value(city)).execute();
            int expectedCount = expectedCounts.get(city);
            assertEquals("Counts are unexpected for city: " + city, expectedCount, r.size());
        }
    }
    
    private void changeCityCount(String city, Map<String, Integer> cityCounts, boolean increment) {
        int count = 0;
        if(cityCounts.containsKey(city)) {
            count = cityCounts.get(city);
        }
        if(increment) {
            count++;
        } else {
            count--;
        }
        cityCounts.put(city, count);
    }
	
	@Test
	public void testPersistClosePersist() throws Exception {
		DBSession session = sessionFactory.createSession();
		EntityBag bag = session.createOrGetBag("testbag");

		List<String> entities = new ArrayList<>();
		try {
			for(int i=0; i<INITIAL_SIZE; i++) {
                Entity entity = bag.addEntity(new SimpleEntity().addProperty("testfield", "test" + i));
				entities.add(entity.getInternalId());
			}
		} finally {
            JasDBMain.shutdown();
		}

        JasDBMain.start();
		session = sessionFactory.createSession();
		bag = session.createOrGetBag("testbag");
		try {
			for(int i=0; i<NUMBER_ENTITIES; i++) {
                Entity entity = bag.addEntity(new SimpleEntity().addProperty("testfield", "test" + (i + INITIAL_SIZE)));
				entities.add(entity.getInternalId());			
			}
			
			for(String id : entities) {
                Entity entity = bag.getEntity(id);
				Assert.assertNotNull("Entity for id: " + id + " should be found", entity);
				assertEquals("Id should match expected id", id, entity.getInternalId());
				Assert.assertNotNull("There should be a property doubleId", entity.getProperty("testfield").getFirstValueObject());
			}
			
			int recordsFound = 0;
			for(Entity en : bag.getEntities()) {
				log.debug("Loaded entity: {}", en.getInternalId());
				recordsFound++;
			}
			
			assertEquals("Unexpected amount of entities", INITIAL_SIZE + NUMBER_ENTITIES, recordsFound);
		} finally {
            JasDBMain.shutdown();
		}
	}

    @Test
    public void testInsertOrUpdatePersist() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("insertOrUpdateBag");

        SimpleEntity entity = new SimpleEntity();
        entity.addProperty("Test", "value1");
        String id = bag.persist(entity).getInternalId();

        assertThat(bag.getEntities().size(), is(1L));
        assertThat(bag.getEntity(id).getProperty("Test").getFirstValueObject(), is("value1"));

        SimpleEntity updatedEntity = new SimpleEntity(id);
        updatedEntity.addProperty("AnotherProperty", "AnotherValue");
        bag.persist(updatedEntity);

        assertThat(bag.getEntities().size(), is(1L));
        assertThat(bag.getEntity(id).getProperty("AnotherProperty").getFirstValueObject(), is("AnotherValue"));
    }

    @Test
    public void testEnsureAndRemoveIndex() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");
        bag.ensureIndex(new SimpleIndexField("field1", new StringKeyType()), true);
        bag.ensureIndex(new SimpleIndexField("field2", new StringKeyType()), false);
        bag.addEntity(new SimpleEntity().addProperty("field1", "value1").addProperty("field2", "testkey2value"));

        String jasdbHome = storageLocation + "/.jasdb";
        File field1Index = new File(jasdbHome, "testbag_field1.idx");
        File field2Index = new File(jasdbHome, "testbag_field2ID.idx");
        assertTrue("Index 1 file should exist", field1Index.exists());
        assertTrue("Index 2 file should exist", field2Index.exists());

        bag.removeIndex("field1");
        assertFalse("Index 1 file should no longer exist", field1Index.exists());
        assertTrue("Index 2 file should exist", field2Index.exists());

        bag.removeIndex("field2ID");
        assertFalse("Index 1 file should no longer exist", field1Index.exists());
        assertFalse("Index 2 file should no longer exist", field2Index.exists());
    }

    @Test
    public void testComplexEntityStorage() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");

        String documentId;
        try {
            SimpleEntity entity = new SimpleEntity();
            entity.setProperty("simpleProperty1", 100L);
            entity.setProperty("multiValueProperty", "value1", "value2", "value3");
            entity.setProperty("integerProperty", 200);

            EmbeddedEntity embeddedEntity = new EmbeddedEntity();
            embeddedEntity.setProperty("embeddedProperty1", 50L, 60L, 70L);
            embeddedEntity.setProperty("embeddedString", "simpleStringValue");
            embeddedEntity.setProperty("embeddedMultivalue", "emValue1", "emValue2", "emValue3");
            entity.addEntity("embedded", embeddedEntity);
            bag.addEntity(entity);

            documentId = entity.getInternalId();
        } finally {
            JasDBMain.shutdown();
        }

        assertNotNull(documentId);

        //reinitialize the db
        JasDBMain.start();
        session = sessionFactory.createSession();
        bag = session.createOrGetBag("testbag");

        Entity foundEntity = bag.getEntity(documentId);
        assertEquals("Internal Doc id's should be the same", documentId, foundEntity.getInternalId());

        assertTrue(foundEntity.hasProperty("simpleProperty1"));
        assertTrue(foundEntity.hasProperty("multiValueProperty"));
        assertTrue(foundEntity.hasProperty("integerProperty"));
        assertEquals(new Long(100), foundEntity.getProperty("simpleProperty1").getFirstValueObject());
        assertEquals(3, foundEntity.getProperty("multiValueProperty").getValues().size());
        assertEquals("value1", foundEntity.getProperty("multiValueProperty").getValues().get(0).getValue());
        assertEquals("value2", foundEntity.getProperty("multiValueProperty").getValues().get(1).getValue());
        assertEquals("value3", foundEntity.getProperty("multiValueProperty").getValues().get(2).getValue());
        assertEquals(new Long(200), foundEntity.getProperty("integerProperty").getFirstValueObject());

        assertTrue("There should be an embedded entity", foundEntity.hasProperty("embedded"));
        Object embedded = foundEntity.getProperty("embedded").getFirstValueObject();
        assertNotNull(embedded);
        assertEquals("Embedded entity should be of type SimpleEntity", EmbeddedEntity.class, embedded.getClass());

        EmbeddedEntity embeddedEntity = (EmbeddedEntity)embedded;
        assertTrue(embeddedEntity.hasProperty("embeddedProperty1"));
        assertTrue(embeddedEntity.hasProperty("embeddedString"));
        assertTrue(embeddedEntity.hasProperty("embeddedMultivalue"));
        assertEquals(50L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(0).getValue());
        assertEquals(60L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(1).getValue());
        assertEquals(70L, embeddedEntity.getProperty("embeddedProperty1").getValues().get(2).getValue());
        assertEquals("simpleStringValue", embeddedEntity.getProperty("embeddedString").getFirstValueObject());
        assertEquals("emValue1", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(0).getValue());
        assertEquals("emValue2", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(1).getValue());
        assertEquals("emValue3", embeddedEntity.getProperty("embeddedMultivalue").getValues().get(2).getValue());
    }

    @Test
    public void testBagFlush() throws JasDBException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");

        long sizeBefore = bag.getDiskSize();
        int TEST_SIZE = 1000;
        for(int i=0; i<TEST_SIZE; i++) {
            bag.addEntity(new SimpleEntity());
        }
        bag.flush();

        assertTrue(bag.getDiskSize() > sizeBefore);
    }

    @Test
    public void testPersistIndexNonUniqueQuery() throws JasDBException, InterruptedException {
        DBSession session = sessionFactory.createSession();
        EntityBag bag = session.createOrGetBag("testbag");

        bag.addEntity(new SimpleEntity().addProperty("city", "Amsterdam"));
        bag.addEntity(new SimpleEntity().addProperty("city", "Amsterdam"));
        bag.addEntity(new SimpleEntity().addProperty("city", "Rotterdam"));
        bag.addEntity(new SimpleEntity().addProperty("city", "Utrecht"));
        bag.addEntity(new SimpleEntity().addProperty("city", "Utrecht"));

        QueryResult result = bag.find(QueryBuilder.createBuilder().field("city").value("Amsterdam")).execute();
        assertThat(result.size(), is(2L));
        result.close();

        bag.ensureIndex(new SimpleIndexField("city", new StringKeyType()), false);

        //let's give the index some time to build
        Thread.sleep(5000);

        result = bag.find(QueryBuilder.createBuilder().field("city").value("Amsterdam")).execute();
        assertThat(result.size(), is(2L));
        result.close();
    }
}
