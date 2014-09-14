/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.collections;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * User: renarj
 * Date: 3/2/12
 * Time: 4:08 PM
 */
public class OrderedBalanceTreeTest {
    private Logger log = LoggerFactory.getLogger(OrderedBalanceTreeTest.class);
    private static final Random rnd = new Random(System.currentTimeMillis());

    @Test(expected = NoSuchElementException.class)
    public void testBalanceTreePreviousNonExisting() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        balancedTree.put(1L, 1L);
        balancedTree.put(3L, 3L);
        balancedTree.put(5L, 5L);
        balancedTree.put(6L, 6L);
        balancedTree.put(10L, 10L);
        balancedTree.put(15L, 15L);

        balancedTree.previous(16L);
    }

    @Test(expected = NoSuchElementException.class)
    public void testBalanceTreeNextNonExisting() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        balancedTree.put(1L, 1L);
        balancedTree.put(3L, 3L);
        balancedTree.put(5L, 5L);
        balancedTree.put(6L, 6L);
        balancedTree.put(10L, 10L);
        balancedTree.put(15L, 15L);

        balancedTree.next(16L);
    }

    @Test
    public void testSplit() throws InterruptedException {
        int testSize = 512;
        OrderedBalancedTree<Long, Long> ol = createTree();
        for(int i=0; i<testSize; i++) {
            Long val = new Long(i);
            ol.put(val, val);
        }

        long start = System.nanoTime();
        List<Long>[] r = ol.split();
        long end = System.nanoTime();
        log.info("First list: {} second list: {}", r[0].size(), r[1].size());
        log.info("Split time took: {}", (end - start));

        int counter = 0;
        for(Long val : r[0]) {
            assertEquals("Unexpected value", new Long(counter), val);
            counter++;
        }
        for(Long val : r[1]) {
            assertEquals("Unexpected value", new Long(counter), val);
            counter++;
        }
    }

    @Test
    public void testAddRemove() {
        int testSize = 10000;
        OrderedBalancedTree<Long, Long> ol = createTree();

        List<Long> alreadyGenerated = new ArrayList<>();
        for(int i=0; i<testSize; i++) {
            Long val = new Long(i);
            ol.put(val, val);
            alreadyGenerated.add(val);
        }
        assertList(alreadyGenerated, ol.values());

        long totalTime = 0;
        for(int i=0; i<testSize; i++) {
            long startRemove = System.nanoTime();
            ol.remove(new Long(i));
            long endRemove = System.nanoTime();
            totalTime += (endRemove - startRemove);

            alreadyGenerated.remove(0);
            assertList(alreadyGenerated, ol.values());
        }
        log.info("Average remove time took: {}", (totalTime / testSize));

        assertEquals("There should no longer be any nodes", 0, ol.values().size());
    }


    private void assertList(List<Long> expecteds, List<Long> actuals) {
        assertEquals("Unexpected list size", expecteds.size(), actuals.size());
        for(int i=0; i<actuals.size(); i++) {
            Long expected = actuals.get(i);
            Long actual = expecteds.get(i);

            assertEquals("Unexpected number", expected, actual);
        }

    }

    protected OrderedBalancedTree<Long, Long> createTree() {
        return new OrderedBalancedTree<>();
    }

    @Test
    /**
     * This tests a failure case seen in the invertedindexblock where ordering was incorrect after remove causing a failure in the contains operation
     */
    public void testBalanceTreeRemove() {
        OrderedBalancedTree<String, String> balancedTree = new OrderedBalancedTree<>();
        balancedTree.put("1372;372d13bc-2047-4ce2-bef6-1d345103b37d;", "");
        balancedTree.put("2990;c25d16e1-726e-482a-8eee-7093d53b6ed0;", "");
        balancedTree.put("4017;d5ebbddc-ffdf-411f-b96c-40a7005472c9;", "");
        balancedTree.put("5340;54c898f0-ec53-45c4-8f91-287ad26c6fe3;", "");
        balancedTree.put("6224;8cc7ed3d-9f1a-459e-8428-fd47ebcf6fb3;", "");
        balancedTree.remove("2990;c25d16e1-726e-482a-8eee-7093d53b6ed0;");
        assertTrue("Key should be present after remove", balancedTree.contains("4017;d5ebbddc-ffdf-411f-b96c-40a7005472c9;"));
    }

    @Test
    public void testBalanceTreeReset() {
        OrderedBalancedTree<Long, Long> bTree = createTree();

        for(int i=0; i<100; i++) {
            bTree.put(new Long(i), new Long(i));
        }
        assertEquals("There should be 100 items in the tree", 100, bTree.keys().size());
        assertEquals("There should be 100 items in the tree", 100, bTree.values().size());

        bTree.reset();
        assertEquals("There should be 0 items in the tree", 0, bTree.keys().size());
        assertEquals("There should be 0 items in the tree", 0, bTree.values().size());
    }

    @Test
    public void testAddBalanceTree() {
        List<Long> expectedNumbers = new ArrayList<>();

        OrderedBalancedTree<Long, Long> balanceTree = createTree();
        balanceTree.put(20L, 20L);
        expectedNumbers.add(20L);

        balanceTree.put(8L, 8L);
        expectedNumbers.add(8L);
        balanceTree.put(2L, 2L);
        expectedNumbers.add(2L);
        balanceTree.put(5L, 5L);
        expectedNumbers.add(5L);
        balanceTree.put(6L, 6L);
        expectedNumbers.add(6L);
        balanceTree.put(12L, 12L);
        expectedNumbers.add(12L);
        balanceTree.put(13L, 13L);
        expectedNumbers.add(13L);
        balanceTree.put(17L, 17L);
        expectedNumbers.add(17L);
        balanceTree.put(19L, 19L);
        expectedNumbers.add(19L);
        balanceTree.put(1L, 1L);
        expectedNumbers.add(1L);
        balanceTree.put(0L, 0L);
        expectedNumbers.add(0L);
        balanceTree.put(7L, 7L);
        expectedNumbers.add(7L);
        balanceTree.put(18L, 18L);
        expectedNumbers.add(18L);

        log.debug("Tree: {}", balanceTree);

        Collections.sort(expectedNumbers);
        List<Long> items = balanceTree.values();
        assertList(expectedNumbers, items);
    }

    @Test
    public void testBalanceTreeDuplicates() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        balancedTree.put(1L, 1L);
        balancedTree.put(3L, 3L);
        balancedTree.put(5L, 5L);
        balancedTree.put(6L, 6L);
        balancedTree.put(3L, 3L);
        balancedTree.put(3L, 3L);
        balancedTree.put(5L, 5L);

        List<Long> expected = new ArrayList<>();
        expected.add(1L);
        expected.add(3L);
        expected.add(3L);
        expected.add(3L);
        expected.add(5L);
        expected.add(5L);
        expected.add(6L);

        List<Long> values = balancedTree.values();
        assertEquals("There should be 7 values", 7, values.size());
        assertList(expected, values);
    }

    @Test
    public void testBalanceTreeNextPrevious() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        balancedTree.put(1L, 1L);
        balancedTree.put(3L, 3L);
        balancedTree.put(5L, 5L);
        balancedTree.put(6L, 6L);
        balancedTree.put(10L, 10L);
        balancedTree.put(15L, 15L);

        Assert.assertNull("No previous no expected result", balancedTree.previous(1L));
        assertEquals("Unexpected previous value", 1l, (long) balancedTree.previous(3L));
        assertEquals("Unexpected previous value", 3l, (long) balancedTree.previous(5L));
        assertEquals("Unexpected previous value", 5l, (long) balancedTree.previous(6L));
        assertEquals("Unexpected previous value", 6l, (long) balancedTree.previous(10L));
        assertEquals("Unexpected previous value", 10l, (long) balancedTree.previous(15L));

        assertEquals("Unexpected next value", 3l, (long) balancedTree.next(1L));
        assertEquals("Unexpected previous value", 5l, (long) balancedTree.next(3L));
        assertEquals("Unexpected previous value", 6l, (long) balancedTree.next(5L));
        assertEquals("Unexpected previous value", 10l, (long) balancedTree.next(6L));
        assertEquals("Unexpected previous value", 15l, (long) balancedTree.next(10L));
        Assert.assertNull("No next expected result", balancedTree.next(15L));
    }

    @Test
    public void testBalanceTreeRange() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        int nrElements = 1001;
        for(int i=0; i<nrElements; i++) {
            balancedTree.put((long)i, (long)i);
        }

        int testBatches = 20;
        long averageSearch = 0;
        for(int j=0; j<testBatches; j++) {
            int batchSize = 10;
            long totalTime = 0;
            for(int i=0; i<nrElements - 1; i += batchSize) {
                long start = i;
                long end = start + batchSize;
                totalTime += assertRange(balancedTree, start, end, batchSize + 1, true);
            }
            averageSearch += (totalTime / (nrElements / batchSize));
        }
        log.info("Average trial run time took: {}", (averageSearch / testBatches));
    }

    @Test
    public void testBalanceRangeNoEnd() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        int nrElements = 1000;
        for(int i=0; i<nrElements; i++) {
            balancedTree.put((long)i, (long)i);
        }

        List<Long> results = balancedTree.range(100l, true, null, true);
        assertEquals("Unepxected amount of results", 900, results.size());
    }

    @Test
    public void testBalanceRangeNoStart() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        int nrElements = 1000;
        for(int i=0; i<nrElements; i++) {
            balancedTree.put((long)i, (long)i);
        }

        List<Long> results = balancedTree.range(null, true, 100l, false);
        assertEquals("Unepxected amount of results", 100, results.size());
    }

    @Test
    public void testBalanceTreeRangeExclusive() {
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        int nrElements = 1001;
        for(int i=0; i<nrElements; i++) {
            balancedTree.put((long)i, (long)i);
        }

        int testBatches = 20;
        long averageSearch = 0;
        for(int j=0; j<testBatches; j++) {
            int batchSize = 10;
            long totalTime = 0;
            for(int i=0; i<nrElements - 1; i += batchSize) {
                long start = i;
                long end = start + batchSize;
                totalTime += assertRange(balancedTree, start, end, batchSize - 1, false);
            }
            averageSearch += (totalTime / (nrElements / batchSize));
        }
        log.info("Average trial run time took: {}", (averageSearch / testBatches));
    }

    private long assertRange(OrderedBalancedTree<Long, Long> tree, long start, long end, int expectedAmount, boolean inclusive) {
        long startTime = System.nanoTime();
        List<Long> values = tree.range(start, inclusive, end, inclusive);
        long endTime = System.nanoTime();
        assertEquals("Unexpected amount of results after range", expectedAmount, values.size());

        long currentExpected = inclusive ? start : start + 1;
        for(int i=0; i<expectedAmount; i++) {
            assertEquals("Unexpected value in the range", currentExpected, (long) values.get(i));
            currentExpected++;
        }

        return (endTime - startTime);
    }


    @Test
    public void testBalancedTreeKeys() {
        int testSize = 100;
        List<Long> expected = new ArrayList<>();
        OrderedBalancedTree<Long, Long> balancedTree = createTree();
        for(int i=0; i<testSize; i++) {
            balancedTree.put(new Long(i), new Long(i + testSize));
            expected.add(new Long(i));
        }

        List<Long> actual = balancedTree.keys();
        assertList(expected, actual);
    }

    @Test
    public void testOrder() {
        int testSize = 10000;
        int maxNumber = 1000000;
        List<Long> alreadyGenerated = new ArrayList<>();
        OrderedBalancedTree<Long, Long> ol = createTree();


        long totalTime = 0;
        for(int i=0; i<testSize; i++) {
            int rndId = -1;
            while(rndId == -1 || alreadyGenerated.contains(rnd)) {
                rndId = rnd.nextInt(maxNumber);
            }
            alreadyGenerated.add(new Long(rndId));

            long startAdd = System.nanoTime();
            ol.put(new Long(rndId), new Long(rndId));
            long endAdd = System.nanoTime();
            totalTime += (endAdd - startAdd);
        }
        log.info("Average add operation took: {}", (totalTime / testSize));

        List<Long> sortedList = new ArrayList<>(alreadyGenerated);
        Collections.sort(sortedList);

        List<Long> items = ol.values();
        assertList(sortedList, items);
    }

    @Test
    public void testGetOperations() {
        int testSize = 1000;
        OrderedBalancedTree<Long, Long> ol = createTree();

        for(long i=0; i<testSize; i++) {
            ol.put(i, i);
        }

        Assert.assertFalse(ol.contains(testSize + 1l));

        assertThat(ol.first(), notNullValue());
        assertThat(ol.first(), is(0l));
        assertThat(ol.firstKey(), is(0l));

        assertThat(ol.last(), notNullValue());
        assertThat(ol.last(), is(testSize - 1l));
        assertThat(ol.lastKey(), is(testSize - 1l));

        long totalTime = 0;
        for(long i=0; i<testSize; i++) {
            assertTrue(ol.contains(i));
            long start = System.nanoTime();
            Long ret = ol.get(i);
            long end = System.nanoTime();
            totalTime += (end - start);

            assertThat(ret, notNullValue());
            assertThat(ret, is(i));
        }
        log.info("Average get time: {}", (totalTime / testSize));
    }
}
