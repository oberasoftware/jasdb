package com.oberasoftware.jasdb.core.index;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.keys.factory.CompositeKeyFactory;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.core.index.keys.factory.KeyFactoryManager;
import com.oberasoftware.jasdb.core.index.keys.factory.LongKeyFactory;
import com.oberasoftware.jasdb.core.index.keys.factory.StringKeyFactory;
import com.oberasoftware.jasdb.core.index.keys.factory.UUIDKeyFactory;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class KeyFactoryManagerTest {
    @Test
    public void testLoadStringKeyFactory() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("key(stringType:1024);");
        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("key", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof StringKeyFactory);
    }

    @Test
    public void testLoadStringKeyFactoryPrimaryKey() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("__ID(uuidType);");
        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("__ID", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof UUIDKeyFactory);

    }

    @Test
    public void testLoadFieldWithSpaces() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("key has some spaces(stringType:1024);");
        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("key has some spaces", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof StringKeyFactory);
    }

    @Test
    public void testLoadLongKeyFactory() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("longKey(longType);");
        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("longKey", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof LongKeyFactory);
    }

    @Test
    public void testLoadUUIDKeyFactory() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("uuidKey(uuidType);");
        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("uuidKey", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof UUIDKeyFactory);
    }

    @Test
    public void testLoadMultiKeyFactory() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("uuidKey(uuidType);stringKey(stringType:200);");
        assertEquals(2, keyFactories.length);
        assertNotNull(keyFactories[0]);
        assertNotNull(keyFactories[1]);

        KeyFactory keyFactory = keyFactories[0];
        assertEquals("uuidKey", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof UUIDKeyFactory);

        keyFactory = keyFactories[1];
        assertEquals("stringKey", keyFactory.getFieldName());
        assertTrue(keyFactory instanceof StringKeyFactory);
        assertEquals(200, ((StringKeyType)keyFactory.getKeyType()).getMaxSize());
    }

    @Test
    public void testLoadComplexKeyFactory() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("complexType(uuidKey(uuidType);stringKey(stringType:200);)");

        assertEquals(1, keyFactories.length);
        assertNotNull(keyFactories[0]);
        assertTrue(keyFactories[0] instanceof CompositeKeyFactory);
    }

    @Test
    public void testLoadKeyFactoryForKeyType() throws JasDBStorageException {

    }

    @Test
    public void testLoadEmptyHeader() throws JasDBStorageException {
        KeyFactory[] keyFactories = KeyFactoryManager.parseHeader("");
        assertNotNull(keyFactories);
        assertEquals(0, keyFactories.length);
    }
}
