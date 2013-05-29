package nl.renarj.jasdb.service;

import com.google.common.collect.Lists;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.IndexManager;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.storage.RecordWriterFactory;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.service.metadata.BagMeta;
import nl.renarj.jasdb.service.metadata.InstanceMeta;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class LocalStorageServiceFactoryImplTest {
    private static final String TEST_BAG_NAME = "bag1";
    private static final String TEST_INSTANCE = "default";
    private static final String NONEXISTING_BAG = "nonexistingBag";
    private static final String NONEXISTING_INSTANCE = "nonexistingInstance";

    private StorageServiceFactory storageServiceFactory;
    private Configuration configuration;
    private RecordWriterFactory recordWriterFactory;
    private MetadataStore metadataStore;

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());
        DBBaseTest.cleanData();

        configuration = mock(Configuration.class);
        recordWriterFactory = mock(RecordWriterFactory.class);
        metadataStore = mock(MetadataStore.class);
        KernelContext kernelContext = mock(KernelContext.class);

        when(recordWriterFactory.createWriter(any(File.class))).thenReturn(mock(RecordWriter.class));
        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);
        when(kernelContext.getConfiguration()).thenReturn(configuration);
        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);
        when(metadataStore.isLastShutdownClean()).thenReturn(true);

        storageServiceFactory = new LocalStorageServiceFactoryImpl(configuration, recordWriterFactory);
        storageServiceFactory.initializeServices(kernelContext);
    }

    @After
    public void tearDown() throws Exception {
        storageServiceFactory.shutdownServiceFactory();
        DBBaseTest.cleanData();
    }

    @Test
    public void testGetIndexManager() throws Exception {
        when(metadataStore.containsInstance("instance")).thenReturn(true);
        when(metadataStore.containsInstance("anotherInstance")).thenReturn(true);

        Instance instance = new InstanceMeta("instance", DBBaseTest.jasdbDir.toString());
        IndexManager indexManager = storageServiceFactory.getIndexManager(instance);
        assertNotNull(indexManager);

        Instance anotherInstance = new InstanceMeta("anotherInstance", DBBaseTest.jasdbDir.toString());
        IndexManager anotherIndexManager = storageServiceFactory.getIndexManager(anotherInstance);
        assertNotNull(anotherIndexManager);
        assertNotSame(indexManager, anotherIndexManager);
    }

    @Test
    public void testGetStorageService() throws Exception {
        Instance testInstance = new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString());
        BagMeta bagMeta = new BagMeta(TEST_INSTANCE, TEST_BAG_NAME, new ArrayList<IndexDefinition>());
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(testInstance));
        List<Bag> bags = new ArrayList<Bag>();
        bags.add(bagMeta);
        when(metadataStore.getBags(TEST_INSTANCE)).thenReturn(bags);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));
        when(metadataStore.getBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(bagMeta);

        storageServiceFactory.initializeInstanceBags(TEST_INSTANCE);

        StorageService storageService = storageServiceFactory.getStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertEquals(0, storageService.getIndexNames().size());

        StorageService nonExistingStorageService = storageServiceFactory.getStorageService(TEST_INSTANCE, NONEXISTING_BAG);
        assertNull(nonExistingStorageService);

        nonExistingStorageService = storageServiceFactory.getStorageService(NONEXISTING_INSTANCE, NONEXISTING_BAG);
        assertNull(nonExistingStorageService);
    }

    @Test
    public void testGetOrCreateStorageService() throws Exception {
        Instance testInstance = new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString());
        BagMeta bagMeta = new BagMeta(TEST_INSTANCE, TEST_BAG_NAME, new ArrayList<IndexDefinition>());
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(testInstance));
        List<Bag> bags = new ArrayList<Bag>();
        bags.add(bagMeta);
        when(metadataStore.getBags(TEST_INSTANCE)).thenReturn(bags);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));
        when(metadataStore.getBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(bagMeta);
        when(metadataStore.getBag(TEST_INSTANCE, NONEXISTING_BAG)).thenReturn(bagMeta);

        storageServiceFactory.initializeInstanceBags(TEST_INSTANCE);


        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertEquals(0, storageService.getIndexNames().size());

        StorageService newStorageservice = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, NONEXISTING_BAG);
        assertNotNull(newStorageservice);
        assertEquals(0, newStorageservice.getIndexNames().size());
    }

    @Test(expected = JasDBStorageException.class)
    public void testNonExistingInstanceCreateStorageService() throws Exception {
        storageServiceFactory.getOrCreateStorageService(NONEXISTING_INSTANCE, NONEXISTING_BAG);
    }

    @Test
    public void testRemoveStorageService() throws Exception {
        Instance testInstance = new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString());
        BagMeta bagMeta = new BagMeta(TEST_INSTANCE, TEST_BAG_NAME, new ArrayList<IndexDefinition>());
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(testInstance));
        List<Bag> bags = new ArrayList<Bag>();
        bags.add(bagMeta);
        when(metadataStore.getBags(TEST_INSTANCE)).thenReturn(bags);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));
        when(metadataStore.getBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(bagMeta);
        when(metadataStore.getBag(TEST_INSTANCE, NONEXISTING_BAG)).thenReturn(bagMeta);

        storageServiceFactory.initializeInstanceBags(TEST_INSTANCE);

        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);

        File bag1 = new File(DBBaseTest.jasdbDir, "bag1.pjs");
        assertTrue(bag1.createNewFile());

        storageServiceFactory.removeStorageService(TEST_INSTANCE, TEST_BAG_NAME);

        storageService = storageServiceFactory.getStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNull(storageService);
        assertFalse(bag1.exists());
    }

    @Test
    public void testRemoveAllStorageServices() throws Exception {
        Instance testInstance = new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString());
        BagMeta bag1Meta = new BagMeta(TEST_INSTANCE, TEST_BAG_NAME, new ArrayList<IndexDefinition>());
        BagMeta bag2Meta = new BagMeta(TEST_INSTANCE, "bag2", new ArrayList<IndexDefinition>());
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(testInstance));
        List<Bag> bags = new ArrayList<Bag>();
        bags.add(bag1Meta);
        bags.add(bag2Meta);
        when(metadataStore.getBags(TEST_INSTANCE)).thenReturn(bags);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));
        when(metadataStore.getBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(bag1Meta);
        when(metadataStore.getBag(TEST_INSTANCE, "bag2")).thenReturn(bag2Meta);

        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, "bag2");
        assertNotNull(storageService);

        File bag1 = new File(DBBaseTest.jasdbDir, "bag1.pjs");
        assertTrue(bag1.createNewFile());

        File bag2 = new File(DBBaseTest.jasdbDir, "bag2.pjs");
        assertTrue(bag2.createNewFile());

        storageServiceFactory.removeAllStorageService(TEST_INSTANCE);

        assertNull(storageServiceFactory.getStorageService(TEST_INSTANCE, TEST_BAG_NAME));
        assertNull(storageServiceFactory.getStorageService(TEST_INSTANCE, "bag2"));
        assertFalse(bag1.exists());
        assertFalse(bag2.exists());
    }
}
