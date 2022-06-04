package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.CacheEntry;
import com.oberasoftware.jasdb.api.caching.CacheRegion;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.core.utils.configuration.ManualConfiguration;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class GlobalCachingMemoryManagerTest {

    public static final long MEMORY_SIZE = 500000l;

    @Test
    public void testCalculateMemorySize() {
        GlobalCachingMemoryManager globalCachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();
        try {
            assertThat(globalCachingMemoryManager.calculateMemorySize(), is(0l));

            CacheRegion<Long, CacheEntry<Long>> region1 = mock(CacheRegion.class);
            CacheRegion<Long, CacheEntry<Long>> region2 = mock(CacheRegion.class);
            CacheRegion<Long, CacheEntry<Long>> region3 = mock(CacheRegion.class);
            when(region1.name()).thenReturn("region1");
            when(region1.memorySize()).thenReturn(5000l);
            when(region2.memorySize()).thenReturn(10000l);
            when(region2.name()).thenReturn("region2");
            when(region3.memorySize()).thenReturn(20000l);
            when(region3.name()).thenReturn("region3");

            globalCachingMemoryManager.registerRegion(region1);
            assertThat(globalCachingMemoryManager.calculateMemorySize(), is(5000l));

            globalCachingMemoryManager.registerRegion(region2);
            globalCachingMemoryManager.registerRegion(region3);
            assertThat(globalCachingMemoryManager.calculateMemorySize(), is(35000l));
        } finally {
            GlobalCachingMemoryManager.shutdown();
        }
    }

    @Test
    public void testMonitorThreadStarted() throws ConfigurationException, InterruptedException {
        GlobalCachingMemoryManager globalCachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();
        try {
            CacheRegion<Long, CacheEntry<Long>> region = mock(CacheRegion.class);
            when(region.memorySize()).thenReturn(MEMORY_SIZE); //0.5 MB
            when(region.reduceBy(anyLong())).thenReturn(MEMORY_SIZE);
            when(region.name()).thenReturn("region1");

            Map<String, String> cachingProperties = new HashMap<>();
            cachingProperties.put("MaxMemory", "2k"); //2KB
            cachingProperties.put("MemoryMonitorInterval", "1s");
            ManualConfiguration manualConfiguration = new ManualConfiguration("caching", cachingProperties);
            globalCachingMemoryManager.configure(manualConfiguration);
            globalCachingMemoryManager.registerRegion(region);

            Thread.sleep(2000);

            verify(region, atLeast(1)).reduceBy(anyLong());
        } finally {
            GlobalCachingMemoryManager.shutdown();
        }
    }

    @Test
    public void testReduceLeastAccessed() {
        GlobalCachingMemoryManager globalCachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();
        try {
            long currentTime = System.currentTimeMillis();
            CacheRegion<Long, CacheEntry<Long>> region1 = mock(CacheRegion.class);
            when(region1.memorySize()).thenReturn(MEMORY_SIZE); //0.5 MB
            when(region1.reduceBy(anyLong())).thenReturn(MEMORY_SIZE);
            when(region1.lastRegionAccess()).thenReturn(currentTime - 1000);
            when(region1.name()).thenReturn("region1");

            CacheRegion<Long, CacheEntry<Long>> region2 = mock(CacheRegion.class);
            when(region2.memorySize()).thenReturn(0l); //0.5 MB
            when(region2.lastRegionAccess()).thenReturn(currentTime);
            when(region2.name()).thenReturn("region2");
            globalCachingMemoryManager.registerRegion(region1);
            globalCachingMemoryManager.registerRegion(region2);

            globalCachingMemoryManager.checkMemoryState(new HashSet<CacheRegion>());

            verify(region1, times(1)).reduceBy(anyLong());
            verify(region2, times(0)).reduceBy(anyLong());
        } finally {
            GlobalCachingMemoryManager.shutdown();
        }
    }
}
