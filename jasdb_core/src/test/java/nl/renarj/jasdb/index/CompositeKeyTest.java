package nl.renarj.jasdb.index;

import nl.renarj.jasdb.core.collections.KeyOrderedTree;
import nl.renarj.jasdb.core.collections.OrderedBalancedTree;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapperImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class CompositeKeyTest {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeKeyTest.class);
    @Test
    public void testPartialLongKeyMatch() {
        int ages = 10;
        int records = 20;

        OrderedBalancedTree<Key, Key> balancedTree = createTree(ages, records);
        for(int i=0; i<ages; i++) {
//            CompositeKey compositeKey = new CompositeKey();
//            compositeKey.addKey(nameMapper, "age", new LongKey(i));
            Key key = new LongKey(i);

            List<Key> range = balancedTree.range(key, true, key, true);
            assertEquals(records, range.size());
        }
    }

    @Test
    public void testCompareBefore() {
        int ages = 10;
        int records = 20;

        KeyOrderedTree<Key> balancedTree = createTree(ages, records);
        KeyNameMapper nameMapper = createNameMapper();

        for(int i=0; i<ages; i++) {
            Key key = balancedTree.getBefore(new CompositeKey().addKey(nameMapper, "age", new LongKey(i))); //new CompositeKey().addKey(nameMapper, "age", new LongKey(i)).addKey(nameMapper, "record", new LongKey(0)));

            assertTrue(key instanceof CompositeKey);

            LOG.info("i: {} Key: {}", i, key);
            CompositeKey compositeKey = (CompositeKey) key;
            if(i == 0) {
                assertEquals(new LongKey(0), compositeKey.getKey(nameMapper, "age"));
                assertEquals(new LongKey(0), compositeKey.getKey(nameMapper, "record"));
            } else {
                assertEquals(new LongKey(i - 1), compositeKey.getKey(nameMapper, "age"));
                assertEquals(new LongKey(records - 1), compositeKey.getKey(nameMapper, "record"));
            }
        }
    }

    private KeyNameMapper createNameMapper() {
        KeyNameMapper nameMapper = new KeyNameMapperImpl();
        nameMapper.addMappedField("age");
        nameMapper.addMappedField("record");
        nameMapper.setValueMarker(2);

        return nameMapper;
    }

    private KeyOrderedTree<Key> createTree(int ages, int records) {
        KeyNameMapper nameMapper = createNameMapper();
        KeyOrderedTree<Key> balancedTree = new KeyOrderedTree<>();
        for(int i=0; i<ages; i++) {

            for(int j=0; j<records; j++) {
                CompositeKey compositeKey = new CompositeKey();
                compositeKey.addKey(nameMapper, "age", new LongKey(i));
                compositeKey.addKey(nameMapper, "record", new LongKey(j));
                balancedTree.put(compositeKey, compositeKey);
            }

        }

        return balancedTree;
    }
}
