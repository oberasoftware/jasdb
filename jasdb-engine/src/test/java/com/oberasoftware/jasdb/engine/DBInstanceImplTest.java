package com.oberasoftware.jasdb.engine;

import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.engine.metadata.InstanceMeta;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Renze de Vries
 */
@RunWith(MockitoJUnitRunner.class)
public class DBInstanceImplTest {

    @Mock
    private StorageServiceFactory storageServiceFactory;

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private InstanceMeta instanceMeta;


    @Test
    public void testGetBag() throws JasDBStorageException {
        Bag bag = mock(Bag.class);

        when(metadataStore.getBag("instance1", "bag")).thenReturn(bag);
        when(instanceMeta.getInstanceId()).thenReturn("instance1");

        DBInstanceImpl dbInstance = new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta);
        Bag retrievedBag = dbInstance.getBag("bag");
        assertThat(retrievedBag, is(bag));
    }

    @Test
    public void testGetBags() throws JasDBStorageException {
        Bag bag1 = mock(Bag.class);
        Bag bag2 = mock(Bag.class);

        List<Bag> bagList = Lists.newArrayList(bag1, bag2);
        when(metadataStore.getBags("instance1")).thenReturn(bagList);
        when(instanceMeta.getInstanceId()).thenReturn("instance1");

        DBInstanceImpl dbInstance = new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta);
        assertThat(dbInstance.getBags().size(), is(2));
        assertThat(dbInstance.getBags(), is(bagList));
    }

    @Test
    public void testRemoveBag() throws JasDBStorageException {
        when(instanceMeta.getInstanceId()).thenReturn("instance1");

        DBInstance dbInstance = new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta);
        dbInstance.removeBag("bag");

        verify(storageServiceFactory, times(1)).removeStorageService("instance1", "bag");
    }

    @Test
    public void testGetPath() {
        when(instanceMeta.getPath()).thenReturn("/some/path");
        DBInstanceImpl dbInstance = new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta);

        assertThat(dbInstance.getPath(), is("/some/path"));
    }

    @Test
    public void testGetInstanceId() {
        when(instanceMeta.getInstanceId()).thenReturn("instance1");
        DBInstanceImpl dbInstance = new DBInstanceImpl(storageServiceFactory, metadataStore, instanceMeta);

        assertThat(dbInstance.getInstanceId(), is("instance1"));
    }
}
