package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class BtreeKeyTypesTest extends IndexBaseTest {
    @Before
    public void setup() {
        cleanData();
    }

    @After
    public void tearDown() {
        cleanData();
    }

    @Test
    public void testIndexUnicodeStringKey() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("string", new StringKeyType(4)), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
        index.insertIntoIndex(new StringKey("暑").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(1)));
        index.insertIntoIndex(new StringKey("変").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(2)));
        index.insertIntoIndex(new StringKey("Ѯ").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(3)));
        index.insertIntoIndex(new StringKey("ß").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(4)));
        index.insertIntoIndex(new StringKey("국").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(5)));
        index.close();

        Assert.assertEquals("Index state should be closed", IndexState.CLOSED, index.getState());
        index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
        try {
            assertKey(new StringKey("暑"), index.searchIndex(new EqualsCondition(new StringKey("暑")), Index.NO_SEARCH_LIMIT));
            assertKey(new StringKey("変"), index.searchIndex(new EqualsCondition(new StringKey("変")), Index.NO_SEARCH_LIMIT));
            assertKey(new StringKey("Ѯ"), index.searchIndex(new EqualsCondition(new StringKey("Ѯ")), Index.NO_SEARCH_LIMIT));
            assertKey(new StringKey("ß"), index.searchIndex(new EqualsCondition(new StringKey("ß")), Index.NO_SEARCH_LIMIT));
            assertKey(new StringKey("국"), index.searchIndex(new EqualsCondition(new StringKey("국")), Index.NO_SEARCH_LIMIT));
        } finally {
            index.close();
        }
    }

    @Test
    public void testStringKeyUUIDIndex() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("uuid", new UUIDKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_uuid.idx"), keyInfo);
        UUID uuid = new UUID(1, 1);
        index.insertIntoIndex(new UUIDKey(uuid).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(1)));
        assertKey(new UUIDKey(uuid), index.searchIndex(new EqualsCondition(new StringKey(uuid.toString())), Index.NO_SEARCH_LIMIT));
        index.close();
    }

    @Test
    public void testStringKeyLongIndex() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("long", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_randomlong.idx"), keyInfo);
        index.insertIntoIndex(new LongKey(50l).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(1)));
        assertKey(new LongKey(50l), index.searchIndex(new EqualsCondition(new StringKey("50")), Index.NO_SEARCH_LIMIT));
        index.close();

    }

    @Test
    public void testLongKeyStringIndex() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("string", new StringKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
        index.insertIntoIndex(new StringKey("50").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(1)));
        assertKey(new StringKey("50"), index.searchIndex(new EqualsCondition(new LongKey(50)), Index.NO_SEARCH_LIMIT));
        index.close();
    }

    @Test
    public void testHashTokenStringIndex() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("string", new StringKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
        index.insertIntoIndex(new StringKey("#").addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(1)));
        assertKey(new StringKey("#"), index.searchIndex(new EqualsCondition(new StringKey("#")), Index.NO_SEARCH_LIMIT));
        index.close();
    }

    private void assertKey(Key expected, IndexSearchResultIterator result) {
        assertEquals(1, result.size());
        assertTrue(result.hasNext());
        assertEquals(expected, result.next());
    }
}
