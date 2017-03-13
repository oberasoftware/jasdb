package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.engine.metadata.BagMeta;
import com.oberasoftware.jasdb.engine.metadata.JasDBMetadataStore;
import com.oberasoftware.jasdb.api.model.Bag;
import com.oberasoftware.jasdb.api.model.IndexDefinition;
import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class JasDBMetadataStoreTest {
    public static final String TEST_INSTANCE_1 = "testInstance1";
    public static final String TEST_INSTANCE_2 = "testInstance2";
    public static final String DEFAULT = "default";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String storeLocation;
    private JasDBMetadataStore metadataStore;

    @Before
    public void before() throws IOException, JasDBStorageException {
        storeLocation = temporaryFolder.newFolder().toString();
        System.setProperty("JASDB_HOME", storeLocation);
        metadataStore = new JasDBMetadataStore();
    }

    @After
    public void after() throws JasDBStorageException {
        metadataStore.closeStore();
    }

    @Test
    public void testCloseOpenMetadataStore() throws JasDBStorageException {
        metadataStore.addInstance(TEST_INSTANCE_1);
        metadataStore.addBag(new BagMeta(TEST_INSTANCE_1, "bag1", new ArrayList<>()));
        metadataStore.addBag(new BagMeta(TEST_INSTANCE_1, "bag2", new ArrayList<>()));
        metadataStore.addBag(new BagMeta(DEFAULT, "bag3", new ArrayList<>()));

        metadataStore.closeStore();
        metadataStore = new JasDBMetadataStore();

        assertThat(metadataStore.getInstances().size(), is(2));
        assertThat(getInstanceIds(metadataStore.getInstances()), hasItems(DEFAULT, TEST_INSTANCE_1));

        assertThat(metadataStore.getBags(DEFAULT).size(), is(1));
        assertThat(getBagNames(metadataStore.getBags(DEFAULT)), hasItems("bag3"));
        assertThat(metadataStore.getBags(TEST_INSTANCE_1).size(), is(2));
        assertThat(getBagNames(metadataStore.getBags(TEST_INSTANCE_1)), hasItems("bag1", "bag2"));
    }

    @Test
    public void testShutdownNotClean() throws JasDBStorageException, IOException {
        metadataStore.closeStore();

        File jasdbHome = new File(storeLocation, ".jasdb");
        File pidFile = new File(jasdbHome, "metadata.pid");
        assertTrue(pidFile.createNewFile());

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
        metadataStore.addInstance(TEST_INSTANCE_1);
        metadataStore.addInstance(TEST_INSTANCE_2);

        List<Instance> instances = metadataStore.getInstances();
        assertThat(instances.size(), is(3));

        List<String> instanceIds = getInstanceIds(instances);
        assertThat(instanceIds, hasItems(TEST_INSTANCE_1, TEST_INSTANCE_2, DEFAULT));
    }

    @Test
    public void testGetInstance() throws JasDBStorageException {
        metadataStore.addInstance(TEST_INSTANCE_1);
        metadataStore.addInstance(TEST_INSTANCE_2);

        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getInstanceId(), is(TEST_INSTANCE_1));
        assertThat(metadataStore.getInstance(TEST_INSTANCE_2).getInstanceId(), is(TEST_INSTANCE_2));
        assertThat(metadataStore.getInstance(DEFAULT).getInstanceId(), is(DEFAULT));

        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getPath(), is(getExpectedPath(TEST_INSTANCE_1)));
        assertThat(metadataStore.getInstance(TEST_INSTANCE_2).getPath(), is(getExpectedPath(TEST_INSTANCE_2)));
        assertThat(metadataStore.getInstance(DEFAULT).getPath(), is(new File(storeLocation, ".jasdb").toString()));
    }

    @Test
    public void testAddInstance() throws JasDBStorageException {
        metadataStore.addInstance(TEST_INSTANCE_1);
        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getInstanceId(), is(TEST_INSTANCE_1));

        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getPath(), is(getExpectedPath(TEST_INSTANCE_1)));
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddExistingInstance() throws JasDBStorageException {
        metadataStore.addInstance(TEST_INSTANCE_1);
        metadataStore.addInstance(TEST_INSTANCE_1);
    }

    @Test
    public void testRemoveInstance() throws JasDBStorageException {
        metadataStore.addInstance(TEST_INSTANCE_1);
        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getInstanceId(), is(TEST_INSTANCE_1));
        assertThat(metadataStore.getInstance(TEST_INSTANCE_1).getPath(), is(getExpectedPath(TEST_INSTANCE_1)));
        assertThat(metadataStore.getInstances().size(), is(2));

        metadataStore.removeInstance(TEST_INSTANCE_1);
        assertThat(metadataStore.getInstances().size(), is(1));
        assertThat(metadataStore.getInstance(TEST_INSTANCE_1), nullValue());
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
        metadataStore.removeInstance(DEFAULT);
    }

    @Test
    public void testGetBags() throws JasDBStorageException {
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));
        metadataStore.addBag(new BagMeta("instance1", "bag2", new ArrayList<>()));

        List<Bag> bags = metadataStore.getBags("instance1");
        assertThat(bags.size(), is(2));
        assertThat(getBagNames(bags), hasItems("bag1", "bag2"));
    }

    @Test
    public void testGetBag() throws JasDBStorageException {
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));

        assertThat(metadataStore.getBag("instance1", "bag1").getName(), is("bag1"));
        assertThat(metadataStore.getBag("instance1", "bag1").getInstanceId(), is("instance1"));
    }

    @Test
    public void testGetBagNotExisting() throws JasDBStorageException {
        assertThat(metadataStore.getBag("instance1", "bag1"), nullValue());
    }

    @Test(expected = JasDBStorageException.class)
    public void testAddBagNotExistingInstance() throws JasDBStorageException {
        metadataStore.addBag(new BagMeta("notExistingInstance", "bag", new ArrayList<>()));
    }

    @Test
    public void testAddBagIndex() throws JasDBStorageException {
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(0));

        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index1", "stringKeyType;", "stringKeyType;", 1));
        metadataStore.addBagIndex("instance1", "bag1", new IndexDefinition("index2", "stringKeyType;", "stringKeyType;", 1));
        assertThat(metadataStore.getBag("instance1", "bag1").getIndexDefinitions().size(), is(2));
        assertThat(getIndexHeaders(metadataStore.getBag("instance1", "bag1").getIndexDefinitions()),
                hasItems("index1/stringKeyType;/stringKeyType;/1/", "index2/stringKeyType;/stringKeyType;/1/"));
    }

    @Test
    public void testContainsIndex() throws JasDBStorageException {
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));
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
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));
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
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));
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
        metadataStore.addInstance("instance1");
        metadataStore.addBag(new BagMeta("instance1", "bag1", new ArrayList<>()));

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

    private String getExpectedPath(String instanceId) {
        return storeLocation + "/.jasdb/" + instanceId;
    }
}
