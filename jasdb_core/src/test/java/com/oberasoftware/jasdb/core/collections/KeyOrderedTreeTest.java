package com.oberasoftware.jasdb.core.collections;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Renze de Vries
 */
public class KeyOrderedTreeTest {
    private static final Logger LOG = LoggerFactory.getLogger(KeyOrderedTreeTest.class);

    @Test
    public void testBeforeOperation() {
        KeyOrderedTree<Key> balancedTree = new KeyOrderedTree<>();
        balancedTree.put(new StringKey("00"), new StringKey("00"));
        balancedTree.put(new StringKey("10"), new StringKey("10"));
        balancedTree.put(new StringKey("2"), new StringKey("2"));
        balancedTree.put(new StringKey("4"), new StringKey("4"));
        balancedTree.put(new StringKey("6"), new StringKey("6"));
        balancedTree.put(new StringKey("8"), new StringKey("8"));
        balancedTree.put(new StringKey("A"), new StringKey("A"));
        balancedTree.put(new StringKey("C"), new StringKey("C"));
        balancedTree.put(new StringKey("E"), new StringKey("E"));

        long start = System.nanoTime();
        Key result = balancedTree.getBefore(new StringKey("00AABB"));
        long end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "00", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("0"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "00", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("01AABB"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "00", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("11BBFF"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "10", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("22AABB"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "2", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("33BBDD"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "2", result.getValue());

        start = System.nanoTime();
        result = balancedTree.getBefore(new StringKey("FABBCC"));
        end = System.nanoTime();
        LOG.info("Before operation took: {}", (end - start));
        assertNotNull("Expected a result", result);
        assertEquals("Unexpected result", "e", result.getValue());
    }

}
