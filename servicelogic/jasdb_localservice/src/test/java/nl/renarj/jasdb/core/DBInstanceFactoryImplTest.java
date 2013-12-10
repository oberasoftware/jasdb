package nl.renarj.jasdb.core;

import com.google.common.collect.Lists;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class DBInstanceFactoryImplTest {
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
    public void testLoadInstances() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);
        Instance instance1 = mock(Instance.class);
        Instance instance2 = mock(Instance.class);

        when(instance1.getInstanceId()).thenReturn("instance1");
        when(instance2.getInstanceId()).thenReturn("instance2");
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(instance1, instance2));
        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        assertThat(instanceFactory.listInstances().size(), is(2));
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1", "instance2"));
    }

    @Test
    public void testAddInstance() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);

        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        instanceFactory.addInstance("instance1", "/some/path");

        ArgumentCaptor<Instance> instanceArgumentCaptor = ArgumentCaptor.forClass(Instance.class);
        verify(metadataStore).addInstance(instanceArgumentCaptor.capture());
        assertThat(instanceArgumentCaptor.getValue().getInstanceId(), is("instance1"));
        assertThat(instanceArgumentCaptor.getValue().getPath(), is("/some/path"));

        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1"));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddInstanceAlreadyExisting() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);

        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);
        when(metadataStore.containsInstance("instance1")).thenReturn(false).thenReturn(true);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        instanceFactory.addInstance("instance1", "/some/path");

        instanceFactory.addInstance("instance1", "/some/other/path");
    }

    @Test
    public void testDeleteInstance() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);

        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);
        instanceFactory.addInstance("instance1", "/some/path");
        assertThat(getInstanceIds(instanceFactory.listInstances()), hasItems("instance1"));

        when(metadataStore.containsInstance("instance1")).thenReturn(true);

        instanceFactory.deleteInstance("instance1");

        verify(metadataStore, times(1)).removeInstance("instance1");
    }

    @Test(expected = JasDBStorageException.class)
    public void testDeleteNotExisting() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);

        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);
        when(metadataStore.containsInstance("notexisting")).thenReturn(false);

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        instanceFactory.deleteInstance("notexisting");
    }

    @Test
    public void testGetInstance() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);
        Instance defaultInstance = mock(Instance.class);
        Instance instance = mock(Instance.class);

        when(defaultInstance.getInstanceId()).thenReturn("default");
        when(instance.getInstanceId()).thenReturn("instance");
        when(instance.getPath()).thenReturn("/some/path");
        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(defaultInstance, instance));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        DBInstance loadedInstance = instanceFactory.getInstance("instance");
        assertThat(loadedInstance, notNullValue());
        assertThat(loadedInstance.getInstanceId(), is("instance"));
        assertThat(loadedInstance.getPath(), is("/some/path"));
    }

    @Test
    public void testGetDefaultInstance() throws JasDBStorageException {
        KernelContext kernelContext = mock(KernelContext.class);
        MetadataStore metadataStore = mock(MetadataStore.class);
        Instance defaultInstance = mock(Instance.class);
        Instance instance = mock(Instance.class);

        when(instance.getInstanceId()).thenReturn("instance");
        when(defaultInstance.getInstanceId()).thenReturn("default");
        when(defaultInstance.getPath()).thenReturn("/some/path");
        when(kernelContext.getMetadataStore()).thenReturn(metadataStore);
        when(metadataStore.getInstances()).thenReturn(Lists.newArrayList(defaultInstance, instance));

        DBInstanceFactoryImpl instanceFactory = new DBInstanceFactoryImpl();
//        instanceFactory.initializeServices(kernelContext);

        DBInstance loadedInstance = instanceFactory.getInstance();
        assertThat(loadedInstance, notNullValue());
        assertThat(loadedInstance.getInstanceId(), is("default"));
        assertThat(loadedInstance.getPath(), is("/some/path"));
    }

    @Test
    public void testListInstances() {

    }

    private List<String> getInstanceIds(List<DBInstance> dbInstances) {
        List<String> instanceIds = new ArrayList<>();
        for(DBInstance dbInstance : dbInstances) {
            instanceIds.add(dbInstance.getInstanceId());
        }
        return instanceIds;
    }
}
