package com.oberasoftware.jasdb.core.index;

import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.UUIDKeyType;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyInfoImplTest {
	private static final Logger log = LoggerFactory.getLogger(KeyInfoImplTest.class);
	
	@Test
	public void testSimpleKeyInfoMatchTest() throws JasDBStorageException {
		KeyInfoImpl keyInfo = new KeyInfoImpl("testField(stringType:200);", "valueField(stringType:200);field1(stringType:1000);");
		Set<String> searchMatchFields = new HashSet<>();
		searchMatchFields.add("testField");
		
		int matchRatio = keyInfo.match(searchMatchFields);
		assertEquals("MatchRatio should be 100", 100, matchRatio);
		
		searchMatchFields.add("valueField");
		matchRatio = keyInfo.match(searchMatchFields);
		assertEquals("MatchRatio should be 100", 100, matchRatio);
	}

    @Test
    public void testKeyMapping() throws JasDBStorageException {
        KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("__ID", new UUIDKeyType()), new SimpleIndexField("field1", new StringKeyType()), new SimpleIndexField("field2", new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        assertEquals(2, keyNameMapper.size());
        assertEquals(0, keyNameMapper.getIndexForField("field1"));
        assertEquals(1, keyNameMapper.getIndexForField("field2"));
        assertEquals("field1", keyNameMapper.getFieldForIndex(0));
        assertEquals("field2", keyNameMapper.getFieldForIndex(1));


        keyInfo = new KeyInfoImpl("testField(stringType:200);", "valueField(stringType:200);field1(stringType:1000);");
        keyNameMapper = keyInfo.getKeyNameMapper();
        assertEquals(2, keyNameMapper.size());
        assertEquals(0, keyNameMapper.getIndexForField("valueField"));
        assertEquals(1, keyNameMapper.getIndexForField("field1"));
        assertEquals("valueField", keyNameMapper.getFieldForIndex(0));
        assertEquals("field1", keyNameMapper.getFieldForIndex(1));
    }
	
	@Test
	public void testSimpleKeyInfoFromHeader() throws JasDBStorageException {
		KeyInfoImpl keyInfo = new KeyInfoImpl("testField(stringType:200);", "");
		Assert.assertNotNull(keyInfo.getKeyFactory());
		assertEquals("stringType", keyInfo.getKeyFactory().getKeyId());
		assertEquals("Unexpected keySize", 200, keyInfo.getKeyFactory().getKeySize());
		assertEquals("Unexpected field name", "testField", keyInfo.getKeyFactory().getFieldName());
		assertEquals("Unexpected amount of fields", 1, keyInfo.getKeyFields().size());
		
	}
	
	@Test
	public void testComplexKeyInfoFromHeader() throws JasDBStorageException {
		KeyInfoImpl keyInfo = new KeyInfoImpl("key(stringType:1024);", "RECORD_POINTER(stringType:1024);DATE_FIELD(stringType:20);");
		Assert.assertNotNull(keyInfo.getKeyFactory());
		assertEquals("stringType", keyInfo.getKeyFactory().getKeyId());
		assertEquals("Unexpected keySize", 1024, keyInfo.getKeyFactory().getKeySize());
		assertEquals("Unexpected field name", "key", keyInfo.getKeyFactory().getFieldName());

		assertEquals("Unexpected amount of values", 2, keyInfo.getValueFields().size());
	}
	
	@Test
	public void testSimpleKeyInfoToHeader() throws JasDBStorageException {
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new StringKeyType(20)));
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new StringKeyType(16)), indexValues);
		
		String keyHeader = keyInfo.keyAsHeader();
		assertEquals("", "ID(stringType:16);", keyHeader);
		String valueHeader = keyInfo.valueAsHeader();
		assertEquals("", "POINTER(stringType:20);", valueHeader);
	}
	
	@Test
	public void testComplexKeyInfoToHeader() throws JasDBStorageException {
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new StringKeyType(20)));
		indexValues.add(new SimpleIndexField("field1", new LongKeyType()));
		indexValues.add(new SimpleIndexField("testValue", new LongKeyType()));
		indexValues.add(new SimpleIndexField("SOMEVALUE", new StringKeyType(203)));
		
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new StringKeyType(188)), indexValues);
		
		String keyHeader = keyInfo.keyAsHeader();
		assertEquals("", "ID(stringType:188);", keyHeader);
		String valueHeader = keyInfo.valueAsHeader();
		assertEquals("", "POINTER(stringType:20);field1(longType);testValue(longType);SOMEVALUE(stringType:203);", valueHeader);
	}
	
	@Test
	public void testReadWriteStringKey() throws JasDBStorageException {
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new StringKeyType(20)));
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new StringKeyType(16)), indexValues);
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		StringKey key = new StringKey("myValue");
        key.addKey(keyInfo.getKeyNameMapper(), "POINTER", new StringKey("998"));
		keyInfo.writeKey(key, 0, buffer);

		Key loadedKey = keyInfo.loadKey(0, buffer);
		log.debug("Loaded key: {}", loadedKey.toString());
		assertEquals("The key value is unexpected", "myvalue", loadedKey.getValue());
		assertEquals("There should be one value payload", 1, loadedKey.getKeys().length);
		
		Key value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "POINTER");
		Assert.assertNotNull("There should be a value object", value);
		assertEquals("Value is unexpected", "998", value.getValue());
	}
	
	@Test
	public void testReadWriteUUIDKey() throws JasDBStorageException {
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new LongKeyType()));
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new UUIDKeyType()), indexValues);
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
        UUID uuid = UUID.randomUUID();
		UUIDKey key = new UUIDKey(uuid);
        key.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(998));
		keyInfo.writeKey(key, 0, buffer);

		Key loadedKey = keyInfo.loadKey(0, buffer);
		log.debug("Loaded key: {}", loadedKey.toString());
		assertEquals("The key value is unexpected", uuid.toString(), loadedKey.getValue());
		assertEquals("There should be one value payload", 1, loadedKey.getKeys().length);
		
		Key value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "POINTER");
		Assert.assertNotNull("There should be a value object", value);
		assertEquals("Value is unexpected", (long) 998, value.getValue());
	}

    @Test
    public void testValeyIndexFieldFromHeader() throws JasDBStorageException {
        KeyInfoImpl keyInfo = new KeyInfoImpl("key(stringType:1024);", "RECORD_POINTER(longType)");
        List<IndexField> indexFields = keyInfo.getIndexValueFields();
        assertEquals(1, indexFields.size());

        IndexField indexField = indexFields.get(0);
        assertEquals("RECORD_POINTER", indexField.getField());
        assertEquals("longType", indexField.getKeyType().getKeyId());
    }
	
	@Test
	public void testReadWriteLongKey() throws JasDBStorageException {
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new LongKeyType()));
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new LongKeyType()), indexValues);
		
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		
		LongKey key = new LongKey(5556);
        key.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(993273));
		keyInfo.writeKey(key, 0, buffer);

		Key loadedKey = keyInfo.loadKey(0, buffer);
		log.debug("Loaded key: {}", loadedKey.toString());
		assertEquals("The key value is unexpected", (long) 5556, loadedKey.getValue());
		assertEquals("There should be one value payload", 1, loadedKey.getKeys().length);
		
		Key value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "POINTER");
		Assert.assertNotNull("There should be a value object", value);
		assertEquals("Value is unexpected", (long) 993273, value.getValue());
	}

    @Test
    public void testLoadCompositeKeyFromIndexFields() throws JasDBStorageException {
        KeyInfoImpl keyInfo = new KeyInfoImpl(
                Lists.newArrayList(new SimpleIndexField("age", new LongKeyType()), new SimpleIndexField("city", new StringKeyType())),
                Lists.newArrayList(new SimpleIndexField("POINTER", new LongKeyType())));
        assertEquals("complexType(age(longType);city(stringType:1024););", keyInfo.keyAsHeader());
        assertEquals("POINTER(longType);", keyInfo.valueAsHeader());

        ByteBuffer buffer = ByteBuffer.allocate(8096);
        CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "age", new LongKey(100));
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "city", new StringKey("Amsterdam"));
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(999));
        keyInfo.writeKey(compositeKey, 0, buffer);

        Key loadedKey = keyInfo.loadKey(0, buffer);
        assertTrue(loadedKey instanceof CompositeKey);
        CompositeKey loadedCompositeKey = (CompositeKey) loadedKey;
        assertEquals(new LongKey(100), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "age"));
        assertEquals(new StringKey("Amsterdam"), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "city"));
        assertEquals(new LongKey(999), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "POINTER"));
    }

    @Test
    public void testLoadCompositeKeyFromHeader() throws JasDBStorageException {
        KeyInfoImpl keyInfo = new KeyInfoImpl("complexType(age(longType);city(stringType:1024););", "POINTER(longType);");
        assertEquals("complexType(age(longType);city(stringType:1024););", keyInfo.keyAsHeader());
        assertEquals("POINTER(longType);", keyInfo.valueAsHeader());

        ByteBuffer buffer = ByteBuffer.allocate(8096);
        CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "age", new LongKey(100));
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "city", new StringKey("Amsterdam"));
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(999));
        keyInfo.writeKey(compositeKey, 0, buffer);

        Key loadedKey = keyInfo.loadKey(0, buffer);
        assertTrue(loadedKey instanceof CompositeKey);
        CompositeKey loadedCompositeKey = (CompositeKey) loadedKey;
        assertEquals(new LongKey(100), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "age"));
        assertEquals(new StringKey("Amsterdam"), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "city"));
        assertEquals(new LongKey(999), loadedCompositeKey.getKey(keyInfo.getKeyNameMapper(), "POINTER"));
    }


    @Test
	public void testComplexKeyInfo() throws JasDBStorageException {
		int numberOfKeys = 10;
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new LongKeyType()));
		indexValues.add(new SimpleIndexField("SOME_VALUE", new StringKeyType(255)));
		
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new LongKeyType()), indexValues);
		log.debug("keySize: {}", keyInfo.getKeySize());
		ByteBuffer buffer = ByteBuffer.allocate(keyInfo.getKeySize() * numberOfKeys);

		int curPosition = 0;
		for(int i=0; i<numberOfKeys; i++) {
			LongKey key = new LongKey(i);
            key.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(1000 + i));
            key.addKey(keyInfo.getKeyNameMapper(), "SOME_VALUE", new StringKey("Just some Random String: " + i));
			
			keyInfo.writeKey(key, curPosition, buffer);
			curPosition += keyInfo.getKeySize();
		}

		curPosition = 0;
		for(int i=0; i<numberOfKeys; i++) {
			long startLoad = System.nanoTime();
			Key loadedKey = keyInfo.loadKey(curPosition, buffer);
			long endLoad = System.nanoTime();
			log.debug("Key was loaded in: {} ns.", (endLoad - startLoad));

			log.debug("Loaded key: {}", loadedKey.toString());
			assertEquals("The key value is unexpected", (long) i, loadedKey.getValue());
			assertEquals("There should be a two value payload", 2, loadedKey.getKeys().length);
			
			Key value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "SOME_VALUE");
			Assert.assertNotNull("There should be a value object", value);
			assertEquals("Value is unexpected", "just some random string: " + i, value.getValue());

			value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "POINTER");
			Assert.assertNotNull("There should be a value object", value);
			assertEquals("Value is unexpected", (long) (1000 + i), value.getValue());
			
			curPosition += keyInfo.getKeySize();
		}
	}
	
	@Test
	public void testSimpleKeyInfo() throws JasDBStorageException {
		int numberOfKeys = 10;
		List<IndexField> indexValues = new ArrayList<>();
		indexValues.add(new SimpleIndexField("POINTER", new LongKeyType()));
		
		KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("ID", new LongKeyType()), indexValues);
		log.debug("keySize: {}", keyInfo.getKeySize());
		ByteBuffer buffer = ByteBuffer.allocate(keyInfo.getKeySize() * numberOfKeys);

		int curPosition = 0;
		for(int i=0; i<numberOfKeys; i++) {
			LongKey key = new LongKey(i);
            key.addKey(keyInfo.getKeyNameMapper(), "POINTER", new LongKey(1000 + i));
			
			keyInfo.writeKey(key, curPosition, buffer);
			curPosition += keyInfo.getKeySize();
		}

		curPosition = 0;
		for(int i=0; i<numberOfKeys; i++) {
			long startLoad = System.nanoTime();
			Key loadedKey = keyInfo.loadKey(curPosition, buffer);
			long endLoad = System.nanoTime();
			log.debug("Key was loaded in: {} ns.", (endLoad - startLoad));

			log.debug("Loaded key: {}", loadedKey.toString());
			assertEquals("The key value is unexpected", (long) i, loadedKey.getValue());
			assertEquals("There should be a one value payload", 1, loadedKey.getKeys().length);
			
			Key value = loadedKey.getKey(keyInfo.getKeyNameMapper(), "POINTER");
			Assert.assertNotNull("There should be a value object", value);
			assertEquals("Value is unexpected", (long) (1000 + i), value.getValue());
			
			curPosition += keyInfo.getKeySize();
		}
	}
}
