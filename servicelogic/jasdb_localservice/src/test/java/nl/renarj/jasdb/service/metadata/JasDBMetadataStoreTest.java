package nl.renarj.jasdb.service.metadata;

import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItems;

/**
 * @author Renze de Vries
 */
public class JasDBMetadataStoreTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private File storeLocation;
    private JasDBMetadataStore metadataStore;

    @Before
    public void before() throws IOException, JasDBStorageException {
        storeLocation = temporaryFolder.newFolder();
        System.setProperty("JASDB_HOME", storeLocation.toString());
        metadataStore = new JasDBMetadataStore();
    }

    @After
    public void after() throws JasDBStorageException {
        metadataStore.closeStore();
    }

    @Test
    public void testCloseOpenMetadataStore() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path"));
        metadataStore.addBag(new BagMeta("testInstance1", "bag1", new ArrayList<IndexDefinition>()));
        metadataStore.addBag(new BagMeta("testInstance1", "bag2", new ArrayList<IndexDefinition>()));
        metadataStore.addBag(new BagMeta("default", "bag3", new ArrayList<IndexDefinition>()));

        metadataStore.closeStore();
        metadataStore = new JasDBMetadataStore();

        assertThat(metadataStore.getInstances().size(), is(2));
        assertThat(getInstanceIds(metadataStore.getInstances()), hasItems("default", "testInstance1"));

        assertThat(metadataStore.getBags("default").size(), is(1));
        assertThat(getBagNames(metadataStore.getBags("default")), hasItems("bag3"));
        assertThat(metadataStore.getBags("testInstance1").size(), is(2));
        assertThat(getBagNames(metadataStore.getBags("testInstance1")), hasItems("bag1", "bag2"));
    }

    @Test
    public void testShutdownNotClean() throws JasDBStorageException, IOException {
        metadataStore.closeStore();

        File jasdbHome = new File(storeLocation, ".jasdb");
        File pidFile = new File(jasdbHome, "metadata.pid");
        pidFile.createNewFile();

        metadataStore = new JasDBMetadataStore();
        assertFalse(metadataStore.isLastShutdownClean());
    }

    @Test
    public void testShutdownClean() throws JasDBStorageException {
        assertTrue(metadataStore.isLastShutdownClean());
    }

    @Test
    public void testClosePidFileRemoved() throws JasDBStorageException {
        File jasdbHome = new File(storeLocation, ".jasdb");
        File pidFile = new File(jasdbHome, "metadata.pid");

        assertTrue(pidFile.exists());

        metadataStore.closeStore();

        assertFalse(pidFile.exists());
    }

    @Test
    public void testGetInstances() throws JasDBStorageException, IOException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path1"));
        metadataStore.addInstance(new InstanceMeta("testInstance2", "/some/path2"));

        List<Instance> instances = metadataStore.getInstances();
        assertThat(instances.size(), is(3));

        List<String> instanceIds = getInstanceIds(instances);
        assertThat(instanceIds, hasItems("testInstance1", "testInstance2", "default"));
    }

    @Test
    public void testGetInstance() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path1"));
        metadataStore.addInstance(new InstanceMeta("testInstance2", "/some/path2"));

        assertThat(metadataStore.getInstance("testInstance1").getInstanceId(), is("testInstance1"));
        assertThat(metadataStore.getInstance("testInstance2").getInstanceId(), is("testInstance2"));
        assertThat(metadataStore.getInstance("default").getInstanceId(), is("default"));

        assertThat(metadataStore.getInstance("testInstance1").getPath(), is("/some/path1"));
        assertThat(metadataStore.getInstance("testInstance2").getPath(), is("/some/path2"));
        assertThat(metadataStore.getInstance("default").getPath(), is(new File(storeLocation, ".jasdb").toString()));
    }

    @Test
    public void testAddInstance() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path1"));
        assertThat(metadataStore.getInstance("testInstance1").getInstanceId(), is("testInstance1"));
        assertThat(metadataStore.getInstance("testInstance1").getPath(), is("/some/path1"));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddExistingInstance() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path1"));
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path2"));
    }

    @Test
    public void testRemoveInstance() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("testInstance1", "/some/path1"));
        assertThat(metadataStore.getInstance("testInstance1").getInstanceId(), is("testInstance1"));
        assertThat(metadataStore.getInstance("testInstance1").getPath(), is("/some/path1"));
        assertThat(metadataStore.getInstances().size(), is(2));

        metadataStore.removeInstance("testInstance1");
        assertThat(metadataStore.getInstances().size(), is(1));
        assertThat(metadataStore.getInstance("testInstance1"), nullValue());
    }

    @Test
    public void testRemoveNotExistingInstance() throws JasDBStorageException {
        expectedException.expect(JasDBStorageException.class);
        expectedException.expectMessage("Unable to delete non existing instance:");
        metadataStore.removeInstance("NonExistingInstance");
    }

    @Test
    public void testCannotRemoveDefault() throws JasDBStorageException {
        expectedException.expect(JasDBStorageException.class);
        metadataStore.removeInstance("default");
    }

    @Test
    public void testGetBags() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));
        metadataStore.addBag(new BagMeta("instance1", "bag2", new ArrayList<IndexDefinition>()));

        List<Bag> bags = metadataStore.getBags("instance1");
        assertThat(bags.size(), is(2));
        assertThat(getBagNames(bags), hasItems("bag1", "bag2"));
    }

    @Test
    public void testGetBag() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));

        assertThat(metadataStore.getBag("instance1", "bag1").getName(), is("bag1"));
        assertThat(metadataStore.getBag("instance1", "bag1").getInstanceId(), is("instance1"));
    }

    @Test
    public void testGetBagNotExisting() throws JasDBStorageException {
        assertThat(metadataStore.getBag("instance1", "bag1"), nullValue());
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddBagNotExistingInstance() throws JasDBStorageException {
        metadataStore.addBag(new BagMeta("notExistingInstance", "bag", new ArrayList<IndexDefinition>()));
    }

    @Test
    public void testAddBagIndex() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(0));

        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(2));
        assertThat(getIndexHeaders(metadataStore.getBag("instance1", "bag1").getIndexDefinitions()),
                hasItems("index1/stringKeyType;/stringKeyType;/1/", "index2/stringKeyType;/stringKeyType;/1/"));
    }

    @Test
    public void testContainsIndex() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(0));

        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1));

        assertTrue(metadataStore.containsIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1)));
        assertTrue(metadataStore.containsIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1)));
        assertFalse(metadataStore.containsIndex("instance1", "bag1", new IndexDefinition("nonExistingIndex", "stringKeyType;", "stringKeyType;", 1)));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddIndexNoBag() throws JasDBStorageException {
        metadataStore.addBagIndex("notExistingInstance", "notExistingBag", new IndexDefinition("index1", "stringKeyType", "stringKeyType", 1));
    }

    @Test
    public void testRemoveBagIndex() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(0));

        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(2));

        metadataStore.removeBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(1));
        assertThat(getIndexHeaders(metadataStore.getBag("instance1", "bag1").getIndexDefinitions()),
                hasItems("index2/stringKeyType;/stringKeyType;/1/"));
    }

    @Test(expected = JasDBStorageException.class)
    public void testRemoveIndexNoBag() throws JasDBStorageException {
        metadataStore.removeBagIndex("notExistingInstance", "notExistingBag", new IndexDefinition("index1", "stringKeyType", "stringKeyType", 1));
    }

    @Test
    public void testRemoveNoSuchIndex() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(0));

        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(2));

        metadataStore.removeBagIndex("instance1", "bag1", new IndexDefinition("nonExistingIndex", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(2));
        assertThat(getIndexHeaders(metadataStore.getBag("instance1", "bag1").getIndexDefinitions()),
                hasItems("index1/stringKeyType;/stringKeyType;/1/", "index2/stringKeyType;/stringKeyType;/1/"));
    }

    @Test
    public void testRemoveBag() throws JasDBStorageException {
        metadataStore.addInstance(new InstanceMeta("instance1", "/some/path"));
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<IndexDefinition>()));

        assertThat(metadataStore.getBags("instance1").size(), is(1));

        metadataStore.removeBag("instance1", "bag1");

        assertThat(metadataStore.getBags("instance1").size(), is(0));
    }

    @Test(expected = JasDBStorageException.class)
    public void testRemoveBagNotExisting() throws JasDBStorageException {
        metadataStore.removeBag("instance1", "notexistingbag");
    }

    private List<String> getIndexHeaders(List<IndexDefinition> indexDefinitions) {
        List<String> indexHeaders = new ArrayList<>();
        for(IndexDefinition indexDefinition : indexDefinitions) {
            indexHeaders.add(indexDefinition.toHeader());
        }
        return indexHeaders;
    }

    private List<String> getBagNames(List<Bag> bags) {
        List<String> bagNames = new ArrayList<>();
        for(Bag bag : bags) {
            bagNames.add(bag.getName());
        }
        return bagNames;
    }

    private List<String> getInstanceIds(List<Instance> instances) {
        List<String> instanceIds = new ArrayList<>();
        for(Instance instance : instances) {
            instanceIds.add(instance.getInstanceId());
        }
        return instanceIds;
    }
}
