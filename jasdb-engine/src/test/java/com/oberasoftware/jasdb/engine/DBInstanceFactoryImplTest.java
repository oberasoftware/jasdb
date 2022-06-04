package com.oberasoftware.jasdb.engine;

import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.engine.MetadataStore;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Renze de Vries
 */
@RunWith(MockitoJUnitRunner.class)
public class DBInstanceFactoryImplTest {

    public static final String INSTANCE_1 = "instance1";
    @Mock
    private MetadataStore metadataStore;

    @Mock
    private StorageServiceFactory storageServiceFactory;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testLoadInstances() throws JasDBStorageException {
        Instance instance1 = mock(Instance.class);
        Instance instance2 = mock(Instance.class);

        when(instance1.getInstanceId()).thenReturn(INSTANCE_1);
        when(instance2.getInstanceId()).thenReturn("instance2");
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(instance1, instance2));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);

        assertThat(instanceFactory.listInstances().size(), is(2));
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems(INSTANCE_1, "instance2"));
    }

    @Test
    public void testAddInstance() throws JasDBStorageException, IOException {
        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        Instance instanceMeta = mock(Instance.class);
        when(instanceMeta.getInstanceId()).thenReturn(INSTANCE_1);
        when(metadataStore.addInstance(INSTANCE_1)).thenReturn(instanceMeta);

        instanceFactory.addInstance(INSTANCE_1);

        verify(metadataStore, times(1)).addInstance(INSTANCE_1);
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems(INSTANCE_1));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddInstanceAlreadyExisting() throws JasDBStorageException {
        when(metadataStore.containsInstance(INSTANCE_1)).thenReturn(false).thenReturn(true);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        instanceFactory.addInstance(INSTANCE_1);

        instanceFactory.addInstance(INSTANCE_1);
    }

    @Test
    public void testDeleteInstance() throws JasDBStorageException, IOException {
        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        Instance instanceMeta = mock(Instance.class);
        when(metadataStore.addInstance(INSTANCE_1)).thenReturn(instanceMeta);
        when(instanceMeta.getInstanceId()).thenReturn(INSTANCE_1);

        instanceFactory.addInstance(INSTANCE_1);

        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems(INSTANCE_1));

        instanceFactory.deleteInstance(INSTANCE_1);

        verify(metadataStore, times(1)).removeInstance(INSTANCE_1);
    }

    @Test(expected = JasDBStorageException.class)
    public void testDeleteNotExisting() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);

        instanceFactory.deleteInstance("notexisting");
    }

    @Test
    public void testGetInstance() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);
        Instance defaultInstance = mock(Instance.class);
        Instance instance = mock(Instance.class);

        when(defaultInstance.getInstanceId()).thenReturn("default");
        when(instance.getInstanceId()).thenReturn("instance");
        when(instance.getPath()).thenReturn("/some/path");
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(defaultInstance, instance));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);

        DBInstance loadedInstance = instanceFactory.getInstance("instance");
        assertThat(loadedInstance, notNullValue());
        assertThat(loadedInstance.getInstanceId(), is("instance"));
        assertThat(loadedInstance.getPath(), is("/some/path"));
    }

    @Test
    public void testGetDefaultInstance() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);
        Instance defaultInstance = mock(Instance.class);
        Instance instance = mock(Instance.class);

        when(instance.getInstanceId()).thenReturn("instance");
        when(defaultInstance.getInstanceId()).thenReturn("default");
        when(defaultInstance.getPath()).thenReturn("/some/path");
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(defaultInstance, instance));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);

        DBInstance loadedInstance = instanceFactory.getInstance();
        assertThat(loadedInstance, notNullValue());
        assertThat(loadedInstance.getInstanceId(), is("default"));
        assertThat(loadedInstance.getPath(), is("/some/path"));
    }

    private List<String> getInstanceIds(List<DBInstance> dbInstances) {
        return dbInstances.stream().map(Instance::getInstanceId).collect(Collectors.toList());
    }
}
