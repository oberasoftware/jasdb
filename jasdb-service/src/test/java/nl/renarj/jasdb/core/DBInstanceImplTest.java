package nl.renarj.jasdb.core;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.platform.HomeLocatorUtil;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.service.metadata.InstanceMeta;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class DBInstanceImplTest {
    @Before
    public void before() throws Exception {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());

        DBBaseTest.cleanData();
    }

    @After
    public void tearDown() throws Exception {
        SimpleKernel.shutdown();
        DBBaseTest.cleanData();
    }

    @Test
    public void testGetBag() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);
        InstanceMeta instanceMeta = mock(InstanceMeta.class);
        Bag bag = mock(Bag.class);

        when(metadataStore.getBag("instance1", "bag")).thenReturn(bag);
        when(instanceMeta.getInstanceId()).thenReturn("instance1");

        DBInstanceImpl dbInstance = new DBInstanceImpl(metadataStore, instanceMeta);
        Bag retrievedBag = dbInstance.getBag("bag");
        assertThat(retrievedBag, is(bag));
    }

    @Test
    public void testGetBags() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);
        InstanceMeta instanceMeta = mock(InstanceMeta.class);
        Bag bag1 = mock(Bag.class);
        Bag bag2 = mock(Bag.class);

        List<Bag> bagList = Lists.newArrayList(bag1, bag2);
        when(metadataStore.getBags("instance1")).thenReturn(bagList);
        when(instanceMeta.getInstanceId()).thenReturn("instance1");

        DBInstanceImpl dbInstance = new DBInstanceImpl(metadataStore, instanceMeta);
        assertThat(dbInstance.getBags().size(), is(2));
        assertThat(dbInstance.getBags(), is(bagList));
    }

    @Test
    public void testRemoveBag() throws JasDBStorageException {
        StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
        storageServiceFactory.getOrCreateStorageService("default", "bag");

        File bagFile = new File(DBBaseTest.jasdbDir, "bag.pjs");
        assertTrue(bagFile.exists());

        DBInstance dbInstance = SimpleKernel.getInstanceFactory().getInstance();
        dbInstance.removeBag("bag");

        assertFalse(bagFile.exists());
    }

    @Test
    public void testGetPath() {
        MetadataStore metadataStore = mock(MetadataStore.class);
        InstanceMeta instanceMeta = mock(InstanceMeta.class);
        when(instanceMeta.getPath()).thenReturn("/some/path");
        DBInstanceImpl dbInstance = new DBInstanceImpl(metadataStore, instanceMeta);

        assertThat(dbInstance.getPath(), is("/some/path"));
    }

    @Test
    public void testGetInstanceId() {
        MetadataStore metadataStore = mock(MetadataStore.class);
        InstanceMeta instanceMeta = mock(InstanceMeta.class);
        when(instanceMeta.getInstanceId()).thenReturn("instance1");
        DBInstanceImpl dbInstance = new DBInstanceImpl(metadataStore, instanceMeta);

        assertThat(dbInstance.getInstanceId(), is("instance1"));
    }
}
