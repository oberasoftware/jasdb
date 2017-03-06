package com.oberasoftware.jasdb.engine;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.api.DBInstance;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private StorageServiceFactory storageServiceFactory;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

//    @Rule
//    public TemporaryFolder temporaryFolder = new TemporaryFolder();

//    @Before
//    public void before() throws Exception {
//        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());
//
//        DBBaseTest.cleanData();
//    }

//    @After
//    public void tearDown() throws Exception {
//        JasDBMain.shutdown();
//        DBBaseTest.cleanData();
//    }

    @Test
    public void testLoadInstances() throws JasDBStorageException {
        Instance instance1 = mock(Instance.class);
        Instance instance2 = mock(Instance.class);

        when(instance1.getInstanceId()).thenReturn("instance1");
        when(instance2.getInstanceId()).thenReturn("instance2");
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(instance1, instance2));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);

        assertThat(instanceFactory.listInstances().size(), is(2));
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1", "instance2"));
    }

    @Test
    public void testAddInstance() throws JasDBStorageException, IOException {
        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        instanceFactory.addInstance("instance1");

        verify(metadataStore, times(1)).addInstance("instance1");
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1"));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddInstanceAlreadyExisting() throws JasDBStorageException {
        when(metadataStore.containsInstance("instance1")).thenReturn(false).thenReturn(true);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        instanceFactory.addInstance("instance1");

        instanceFactory.addInstance("instance1");
    }

    @Test
    public void testDeleteInstance() throws JasDBStorageException, IOException {
        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl(metadataStore, storageServiceFactory);
        instanceFactory.addInstance("instance1");
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1"));

        when(metadataStore.containsInstance("instance1")).thenReturn(true);

        instanceFactory.deleteInstance("instance1");

        verify(metadataStore, times(1)).removeInstance("instance1");
    }

    @Test(expected = JasDBStorageException.class)
    public void testDeleteNotExisting() throws JasDBStorageException {
        MetadataStore metadataStore = mock(MetadataStore.class);

        when(metadataStore.containsInstance("notexisting")).thenReturn(false);

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
        List<String> instanceIds = new ArrayList<>();
        for(DBInstance dbInstance : dbInstances) {
            instanceIds.add(dbInstance.getInstanceId());
        }
        return instanceIds;
    }
}
