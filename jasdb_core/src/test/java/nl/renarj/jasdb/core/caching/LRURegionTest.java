package nl.renarj.jasdb.core.caching;

import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LRURegionTest {
    @Test
    public void testLastRegionAccess() {
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        long lastAccess = region.lastRegionAccess();
        int testSize = 100000;
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(9999, (long)i * 2));

        }

        assertTrue(region.lastRegionAccess() > lastAccess);
    }

    @Test
    public void testRegionPut() {
        int testSize = 100000;
        long blockSize = 9999;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(blockSize, (long)i * 2));

        }
        assertThat(region.size(), is(testSize));
        assertThat(region.memorySize(), is(blockSize * testSize));
    }

    @Test
    public void testRegionGet() {
        int testSize = 100000;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(9999, (long)i * 2));
        }

        for(int i=0; i<testSize; i++) {
            MockCacheEntry<Long> entry = region.getEntry((long)i);
            assertThat(entry.getValue(), is((long)i * 2));
        }
    }

    @Test
    public void testRegionRemove() {
        int testSize = 100000;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(9999, (long)i * 2));
        }

        for(int i=0; i<testSize; i++) {
            region.removeEntry((long)i);
        }

        assertThat(region.size(), is(0));
        assertThat(region.memorySize(), is(0l));

        for(int i=0; i<testSize; i++) {
            assertThat(region.getEntry((long) i), nullValue());
        }
    }

    @Test
    public void testRegionClear() {
        int testSize = 100000;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(9999, (long)i * 2));
        }

        region.clear();
        assertThat(region.size(), is(0));
        assertThat(region.memorySize(), is(0l));

        for(int i=0; i<testSize; i++) {
            assertThat(region.getEntry((long) i), nullValue());
        }
    }

    @Test
    public void testReduceBy() {
        int testSize = 100000;
        long blockSize = 9999;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(blockSize, (long)i * 2));
        }

        region.reduceBy(10 * blockSize);

        assertThat(region.size(), is(testSize - 10));
        assertThat(region.memorySize(), is((testSize - 10) * blockSize));
    }

    @Test
    public void testReduceMoreThanAvailable() {
        int testSize = 100;
        long blockSize = 9999;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(blockSize, (long)i * 2));
        }

        region.reduceBy(1000 * blockSize);

        assertThat(region.size(), is(0));
        assertThat(region.memorySize(), is(0l));
    }

    @Test
    public void testReduceBlocksInUse() {
        int testSize = 100;
        long blockSize = 9999;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(blockSize, (long)i, true));
        }
        long before = region.memorySize();
        long reduced = region.reduceBy(1000 * blockSize);
        assertThat(reduced, is(0l));
        assertThat(region.size(), is(testSize));
        assertThat(region.memorySize(), is(before));
    }

    @Test
    public void testValues() {
        int testSize = 100000;
        long blockSize = 9999;
        LRURegion<MockCacheEntry<Long>> region = new LRURegion<MockCacheEntry<Long>>("region");
        for(int i=0; i<testSize; i++) {
            region.putEntry((long)i, new MockCacheEntry<Long>(blockSize, (long)i));
        }

        Collection<MockCacheEntry<Long>> entries = region.values();
        assertThat(entries.size(), is(testSize));
        int counter = 0;
        for(MockCacheEntry<Long> entry : entries) {
            assertThat(entry.getValue(), is((long)counter));
            counter++;
        }
    }

    private class MockCacheEntry<T> implements CacheEntry<T> {
        private long size;
        private T value;
        private boolean inUse = false;

        private MockCacheEntry(long size, T value) {
            this.size = size;
            this.value = value;
        }

        private MockCacheEntry(long size, T value, boolean inUse) {
            this.size = size;
            this.value = value;
            this.inUse = inUse;
        }

        @Override
        public boolean isInUse() {
            return inUse;
        }

        @Override
        public long memorySize() {
            return size;
        }

        @Override
        public T getValue() {
            return value;
        }
    }
}
