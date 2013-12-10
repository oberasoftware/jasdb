package nl.renarj.storage.index;

import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class IndexManagementTest extends DBBaseTest {

	@After
	public void tearDown() throws JasDBException {
		super.tearDown();
        cleanData();
	}
	
	@Before
	public void setup() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());

        cleanData();
	}
	
	@Test
	public void testLoadIndex() throws JasDBStorageException {
        DBSession pojoDb = new LocalDBSession();
        EntityBag bag = pojoDb.createOrGetBag("testbag");
        bag.ensureIndex(new IndexField("testkey", new StringKeyType()), true, new IndexField("payload", new StringKeyType()));

        DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//		IndexManager instance = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//		try {
//			File indexFile = new File(jasdbDir, "testbag_testkey.idx");
//			Index index = instance.getIndex("testbag", "testkey");
//			Assert.assertNotNull("Index should have been loaded", index);
//			Assert.assertEquals("Index instances should be the same", index.hashCode(), index.hashCode());
//
//			index.flushIndex();
//			assertTrue("There should be an index", indexFile.exists());
//			index.insertIntoIndex(new StringKey("JustSomeKey")
//                    .addKey(index.getKeyInfo().getKeyNameMapper(), "payload", new StringKey("testvalue"))
//                    .addKey(index.getKeyInfo().getKeyNameMapper(), "__ID", new UUIDKey(UUID.randomUUID())));
//			instance.shutdownIndexes();
//
//			index = instance.getIndex("testbag", "testkey");
//			IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new StringKey("JustSomeKey")), new SearchLimit());
//			assertEquals("One key should be present", 1, result.size());
//
//			KeyInfo keyInfo = index.getKeyInfo();
//			assertEquals("The key name should be testkey", "testkey", keyInfo.getKeyName());
//			assertEquals("The page size should be 512", index.getPageSize(), 512);
//		} finally {
//			instance.shutdownIndexes();
//		}
	}

    @Test
    public void testIndexRemove() throws JasDBStorageException {
        DBSession pojoDb = new LocalDBSession();
        EntityBag bag = pojoDb.createOrGetBag("testbag");
        bag.ensureIndex(new IndexField("testkey", new StringKeyType()), true);
        bag.ensureIndex(new IndexField("testkey2", new StringKeyType()), false);
        bag.addEntity(new SimpleEntity().addProperty("testkey", "value1").addProperty("testkey2", "testkey2value"));

        DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//        IndexManager indexManager = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//        Index index = indexManager.getIndex("testbag", "testkey");
//        Index invertedIndex = indexManager.getIndex("testbag", "testkey2ID");
//        File indexFile = new File(jasdbDir, "testbag_testkey.idx");
//        File invertedIndexBtreeFile = new File(jasdbDir, "testbag_testkey2ID.idx");
//        assertEquals("Expected index state to be OK", IndexState.OK, index.getState());
//        assertEquals("Expected index state to be OK", IndexState.OK, invertedIndex.getState());
//        assertTrue("Index file should exist", indexFile.exists());
//        assertTrue("Index file should exist", invertedIndexBtreeFile.exists());
//
//        bag.removeIndex("testkey");
//        assertEquals("Expected index state to be CLOSED", IndexState.CLOSED, index.getState());
//        assertFalse("Index file should no longer exist", indexFile.exists());
//
//        assertEquals("Expected index state to be OK", IndexState.OK, invertedIndex.getState());
//        assertTrue("Index file should exist", invertedIndexBtreeFile.exists());
//
//        bag.removeIndex("testkey2ID");
//        assertEquals("Expected index state to be OK", IndexState.CLOSED, invertedIndex.getState());
//        assertFalse("Index file should exist", invertedIndexBtreeFile.exists());
    }
	
	@Test
	public void testBagIndexCreate() throws Exception {
		DBSession pojoDb = new LocalDBSession();
		EntityBag bag = pojoDb.createOrGetBag("inverted");
		bag.ensureIndex(new IndexField("field1", new StringKeyType(100)), false);
		
		try {
            DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//            IndexManager instance = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//
//			Index loadedIndexField1 = instance.getIndex("inverted", "field1ID");
//			Assert.assertNotNull("Index should have been loaded", loadedIndexField1);
//			KeyInfo field1KeyInfo = loadedIndexField1.getKeyInfo();
//			KeyFactory field1KeyFactory = field1KeyInfo.getKeyFactory();
//			Assert.assertNotNull(field1KeyInfo);
//			Assert.assertNotNull(field1KeyFactory);
//			Assert.assertEquals("The header should match", "complexType(field1(stringType:100);__ID(uuidType););", field1KeyFactory.asHeader());
		} finally {
			SimpleKernel.shutdown();
		}
		
		assertFileExists(new File(jasdbDir, "inverted_field1ID.idx"), true);
	}

    @Test
    public void testBagIndexCompositeCreate() throws Exception {
        DBSession pojoDb = new LocalDBSession();
        EntityBag bag = pojoDb.createOrGetBag("inverted");
        bag.ensureIndex(new CompositeIndexField(new IndexField("field1", new StringKeyType(100)), new IndexField("field2", new LongKeyType())), false);

        try {
            DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//            IndexManager instance = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//
//            Index loadedIndexField = instance.getIndex("inverted", "field1field2ID");
//            Assert.assertNotNull("Index should have been loaded", loadedIndexField);
//            KeyInfo fieldKeyInfo = loadedIndexField.getKeyInfo();
//            KeyFactory field1KeyFactory = fieldKeyInfo.getKeyFactory();
//            Assert.assertNotNull(fieldKeyInfo);
//            Assert.assertNotNull(field1KeyFactory);
//            Assert.assertEquals("The header should match", "complexType(field1(stringType:100);field2(longType);__ID(uuidType););", field1KeyFactory.asHeader());
        } finally {
            SimpleKernel.shutdown();
        }

        assertFileExists(new File(jasdbDir, "inverted_field1field2ID.idx"), true);
    }
	
	@Test
	public void testBestIndexMatch() throws Exception {
		DBSession pojoDb = new LocalDBSession();
		EntityBag bag = pojoDb.createOrGetBag("inverted");
		bag.ensureIndex(new IndexField("field1", new StringKeyType(100)), false);
		bag.ensureIndex(new IndexField("field2", new StringKeyType(100)), false);
		bag.ensureIndex(new IndexField("field3", new StringKeyType(100)), false);
		
		try {
			Set<String> fields = new HashSet<>();
			fields.add("field1");
            DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//            IndexManager instance = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//
//			Index loadedIndex = instance.getBestMatchingIndex("inverted", fields);
//
//			List<String> indexFields = loadedIndex.getKeyInfo().getKeyFields();
//			Assert.assertEquals("There should be one field in the index", 2, indexFields.size());
//			Assert.assertEquals("The field should be field1", "field1", indexFields.get(0));
		} finally {
			SimpleKernel.shutdown();
		}
	}

	@Test
	public void testBagIndexMultiKeyCreate() throws Exception {
		DBSession pojoDb = new LocalDBSession();
		EntityBag bag = pojoDb.createOrGetBag("inverted");
		try {
			bag.ensureIndex(new CompositeIndexField(
					new IndexField("field1", new StringKeyType()), 
					new IndexField("field2", new StringKeyType())
				), false);

            DBInstance dbinstance = SimpleKernel.getInstanceFactory().getInstance();
//            IndexManager instance = SimpleKernel.getStorageServiceFactory().getIndexManager(dbinstance);
//
//            Index index = instance.getIndex("inverted", "field1field2ID");
//            Assert.assertNotNull("Index should have been loaded", index);
//            KeyInfo fieldKeyInfo = index.getKeyInfo();
//            KeyFactory fieldKeyFactory = fieldKeyInfo.getKeyFactory();
//            Assert.assertNotNull(fieldKeyInfo);
//            Assert.assertNotNull(fieldKeyFactory);
//            Assert.assertEquals("The header should match", "complexType(field1(stringType:1024);field2(stringType:1024);__ID(uuidType););", fieldKeyFactory.asHeader());

        } finally {
			SimpleKernel.shutdown();
		}
	}
}
