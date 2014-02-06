package nl.renarj.jasdb.service;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.service.metadata.BagMeta;
import nl.renarj.jasdb.service.metadata.InstanceMeta;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
@RunWith(MockitoJUnitRunner.class)
public class LocalStorageServiceFactoryImplTest {
    private static final String TEST_BAG_NAME = "bag1";
    private static final String TEST_INSTANCE = "default";
    private static final String NONEXISTING_BAG = "nonexistingBag";
    private static final String NONEXISTING_INSTANCE = "nonexistingInstance";

    @Mock
    private Configuration configuration;

    @InjectMocks
    private LocalStorageServiceFactoryImpl storageServiceFactory;

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private ConfigurationLoader configurationLoader;

    @Mock
    private StorageService storageService;

    @Mock
    private StorageService storageService2;

    @Before
    public void setUp() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());
        DBBaseTest.cleanData();

        when(applicationContext.getBean(eq("LocalStorageService"), eq(TEST_INSTANCE), anyString())).thenReturn(storageService).thenReturn(storageService2);


        when(configurationLoader.getConfiguration()).thenReturn(configuration);


        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);
        when(metadataStore.isLastShutdownClean()).thenReturn(true);

    }

    @After
    public void tearDown() throws Exception {
        storageServiceFactory.shutdownServiceFactory();
        DBBaseTest.cleanData();
    }

    @Test
    public void testGetStorageService() throws Exception {
        when(metadataStore.containsBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(true);
        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));

        StorageService storageService = storageServiceFactory.getStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertThat(storageService, is(this.storageService));
        verify(storageService, times(1)).openService(eq(configuration));
        verify(storageService, times(1)).initializePartitions();

        StorageService nonExistingStorageService = storageServiceFactory.getStorageService(TEST_INSTANCE, NONEXISTING_BAG);
        assertNull(nonExistingStorageService);

        nonExistingStorageService = storageServiceFactory.getStorageService(NONEXISTING_INSTANCE, NONEXISTING_BAG);
        assertNull(nonExistingStorageService);
    }

    @Test
    public void testGetOrCreateStorageService() throws Exception {
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));
        when(metadataStore.containsBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(true);
        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);

        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertThat(storageService, is(storageService));
        verify(storageService, times(1)).openService(eq(configuration));
        verify(storageService, times(1)).initializePartitions();


        StorageService newStorageservice = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, NONEXISTING_BAG);
        assertNotNull(newStorageservice);
        assertThat(newStorageservice, is(storageService2));
        verify(newStorageservice, times(1)).openService(eq(configuration));
        verify(newStorageservice, times(1)).initializePartitions();

        ArgumentCaptor<BagMeta> bagMetaArgumentCaptor = ArgumentCaptor.forClass(BagMeta.class);
        verify(metadataStore, times(1)).addBag(bagMetaArgumentCaptor.capture());
        assertThat(bagMetaArgumentCaptor.getValue().getName(), is(NONEXISTING_BAG));

    }

    @Test(expected = JasDBStorageException.class)
    public void testNonExistingInstanceCreateStorageService() throws Exception {
        storageServiceFactory.getOrCreateStorageService(NONEXISTING_INSTANCE, NONEXISTING_BAG);
    }

    @Test
    public void testRemoveStorageService() throws Exception {
        when(metadataStore.containsBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(true);
        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));

        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertThat(storageService, is(storageService));
        verify(storageService, times(1)).openService(eq(configuration));
        verify(storageService, times(1)).initializePartitions();

        storageServiceFactory.removeStorageService(TEST_INSTANCE, TEST_BAG_NAME);

        verify(storageService, times(1)).remove();
        verify(metadataStore, times(1)).removeBag(TEST_INSTANCE, TEST_BAG_NAME);

        when(metadataStore.containsBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(false);
        storageService = storageServiceFactory.getStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNull(storageService);
    }

    @Test
    public void testRemoveAllStorageServices() throws Exception {
        when(metadataStore.containsBag(TEST_INSTANCE, TEST_BAG_NAME)).thenReturn(true);
        when(metadataStore.containsInstance(TEST_INSTANCE)).thenReturn(true);
        when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(new InstanceMeta(TEST_INSTANCE, DBBaseTest.jasdbDir.toString()));

        BagMeta bag1Meta = new BagMeta(TEST_INSTANCE, TEST_BAG_NAME, new ArrayList<IndexDefinition>());
        BagMeta bag2Meta = new BagMeta(TEST_INSTANCE, "bag2", new ArrayList<IndexDefinition>());
        List<Bag> bags = new ArrayList<>();
        bags.add(bag1Meta);
        bags.add(bag2Meta);
        when(metadataStore.getBags(TEST_INSTANCE)).thenReturn(bags);

        StorageService storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, TEST_BAG_NAME);
        assertNotNull(storageService);
        assertThat(storageService, is(this.storageService));
        verify(storageService, times(1)).openService(eq(configuration));
        verify(storageService, times(1)).initializePartitions();

        storageService = storageServiceFactory.getOrCreateStorageService(TEST_INSTANCE, "bag2");
        assertNotNull(storageService);
        assertThat(storageService, is(this.storageService2));
        verify(storageService, times(1)).openService(eq(configuration));
        verify(storageService, times(1)).initializePartitions();

        storageServiceFactory.removeAllStorageService(TEST_INSTANCE);

        verify(this.storageService, times(1)).remove();
        verify(metadataStore, times(1)).removeBag(TEST_INSTANCE, TEST_BAG_NAME);
        verify(this.storageService2, times(1)).remove();
        verify(metadataStore, times(1)).removeBag(TEST_INSTANCE, "bag2");
    }
}
