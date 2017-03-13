package com.oberasoftware.jasdb.core.index;

import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyNameMapperImpl;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Renze de Vries
 */
public class StringKeyTest {
    @Test
    public void testKeyFromBytes() throws UnsupportedEncodingException {
        byte[] stringBytes = "testKey".getBytes("UTF8");
        byte[] unicodeBytes = Arrays.copyOf(stringBytes, 1024);

        StringKey key = new StringKey(unicodeBytes);
        assertEquals("testKey", key.getKey());
        assertEquals(7, key.getUnicodeBytes().length);
    }

    @Test
    public void testKeyFromString() {
        StringKey key = new StringKey("testKey");
        assertEquals("testkey", key.getKey());
        assertEquals(7, key.getUnicodeBytes().length);
    }

    @Test
    public void testAddChildResize() {
        KeyNameMapper mapper = new KeyNameMapperImpl();
        mapper.addMappedField("field1");
        assertEquals(0, mapper.getIndexForField("field1"));

        StringKey key = new StringKey("testKey");
        key.addKey(mapper, "field1", new LongKey(50));
        assertEquals(new LongKey(50), key.getKey(0));
        assertEquals(new LongKey(50), key.getKey(mapper, "field1"));

        mapper.addMappedField("field2");
        assertEquals(1, mapper.getIndexForField("field2"));
        key.addKey(mapper, "field2", new LongKey(200));
        assertEquals(new LongKey(50), key.getKey(0));
        assertEquals(new LongKey(50), key.getKey(mapper, "field1"));
        assertEquals(new LongKey(200), key.getKey(1));
        assertEquals(new LongKey(200), key.getKey(mapper, "field2"));

    }

    @Test
    public void testStringNormalize() {
        StringKey weirdKey = new StringKey("CapitalCasingAlotOfTimesWithSPACESATEND            ");
        assertEquals("capitalcasingalotoftimeswithspacesatend", weirdKey.getKey());
    }

    @Test
    public void testEmptyKey() {
        byte[] emptyBytes = new byte[1024];
        StringKey emptyKey = new StringKey(emptyBytes);
        assertEquals(0, emptyKey.getUnicodeBytes().length);
        assertEquals("", emptyKey.getKey());
    }

    @Test
    public void testLoadSingleCharacter() throws UnsupportedEncodingException {
        byte[] charBytes = "#".getBytes("UTF8");
        byte[] unicodeBytes = Arrays.copyOf(charBytes, 1024);
        StringKey key = new StringKey(unicodeBytes);
        assertEquals("#", key.getKey());
        assertEquals(1, key.getUnicodeBytes().length);
    }

    @Test
    public void testUnicodeKey() throws UnsupportedEncodingException {
        StringKey key = new StringKey("変");
        assertEquals("変", key.getKey());

        byte[] bytes = "変".getBytes("UTF8");
        byte[] unicodeBytes = Arrays.copyOf(bytes, 1024);
        key = new StringKey(unicodeBytes);
        assertEquals("変", key.getKey());
        assertEquals(3, key.getUnicodeBytes().length);
    }
}
