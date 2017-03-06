package nl.renarj.storage.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.api.metadata.MetadataStore;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;
import com.oberasoftware.jasdb.engine.indexing.IndexManagerImpl;
import com.oberasoftware.jasdb.engine.indexing.IndexTypes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IndexManagementTest {

    private static final String TEST_INSTANCE = "DEFAULT_INSTANCE";
    private static final String TESTBAG = "testbag";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    private ConfigurationLoader configurationLoader;

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private Configuration indexConfiguration;

    @InjectMocks
    private IndexManagerImpl indexManager;

    @Before
    public void setup() throws Exception {
        indexManager = new IndexManagerImpl(TEST_INSTANCE);
        MockitoAnnotations.initMocks(this);

        when(configurationLoader.getConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getChildConfiguration(anyString())).thenReturn(null);
    }

    @Test
    public void testCreateIndex() throws JasDBStorageException, IOException {
        try {
            File instanceDirectory = temporaryFolder.newFolder();

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(new ArrayList<IndexDefinition>());
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            Index createdIndex = indexManager.createIndex(TESTBAG, new IndexField("testkey", new StringKeyType()), true, new IndexField("payload", new StringKeyType()));

            File indexFile = new File(instanceDirectory, "testbag_testkey.idx");

            Index index = indexManager.getIndex("testbag", "testkey");
            assertNotNull("Index should have been loaded", index);
            assertEquals("Index instances should be the same", createdIndex.hashCode(), index.hashCode());

            index.flushIndex();
            assertTrue("There should be an index", indexFile.exists());

            index.insertIntoIndex(new StringKey("JustSomeKey")
                    .addKey(index.getKeyInfo().getKeyNameMapper(), "payload", new StringKey("testvalue"))
                    .addKey(index.getKeyInfo().getKeyNameMapper(), "__ID", new UUIDKey(UUID.randomUUID())));

            KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(new IndexField("testkey", new StringKeyType())), Lists.newArrayList(new IndexField("payload", new StringKeyType()), new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType())));
            IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.getIndexType());

            verify(metadataStore, times(1)).addBagIndex(TEST_INSTANCE, TESTBAG, definition);
        } finally {
            indexManager.shutdownIndexes();
        }
    }

    @Test
    public void testLoadIndexes() throws Exception {
        try {
            File instanceDirectory = temporaryFolder.newFolder();

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(new IndexField("testkey", new StringKeyType())), Lists.newArrayList(new IndexField("payload", new StringKeyType()), new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType())));
            IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), IndexTypes.BTREE.getType());

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(Lists.newArrayList(definition));
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Map<String, Index> indexes = indexManager.getIndexes(TESTBAG);
            assertThat(indexes.size(), is(1));

            assertThat(indexes.get(keyInfo.getKeyName()), notNullValue());

            indexes.get(keyInfo.getKeyName()).openIndex();

            assertThat(indexes.get(keyInfo.getKeyName()).getState(), is(IndexState.OK));

            File indexFile = new File(instanceDirectory, "testbag_testkey.idx");
            assertThat(indexFile.exists(), is(true));
        } finally {
            indexManager.shutdownIndexes();
        }
    }

    @Test
    public void testIndexRemove() throws JasDBStorageException, IOException {
        try {
            File instanceDirectory = temporaryFolder.newFolder();

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(new IndexField("testkey", new StringKeyType())), Lists.newArrayList(new IndexField("payload", new StringKeyType()), new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType())));
            IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), IndexTypes.BTREE.getType());

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(Lists.newArrayList(definition));
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Map<String, Index> indexes = indexManager.getIndexes(TESTBAG);
            assertThat(indexes.size(), is(1));
            indexes.get(keyInfo.getKeyName()).openIndex();

            File indexFile = new File(instanceDirectory, "testbag_testkey.idx");
            assertThat(indexFile.exists(), is(true));

            indexManager.removeIndex(TESTBAG, keyInfo.getKeyName());

            assertThat(indexFile.exists(), is(false));
            verify(metadataStore, times(1)).removeBagIndex(TEST_INSTANCE, TESTBAG, definition);
        } finally {
            indexManager.shutdownIndexes();
        }
    }

    @Test
    public void testCreateUniqueComplexIndex() throws JasDBStorageException, IOException {
        try {
            CompositeIndexField complexIndexField = new CompositeIndexField(new IndexField("field1", new StringKeyType(100)), new IndexField("field2", new LongKeyType()));

            File instanceDirectory = temporaryFolder.newFolder();

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(new ArrayList<IndexDefinition>());
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            Index createdIndex = indexManager.createIndex(TESTBAG, complexIndexField, true, new IndexField("payload", new StringKeyType()));

            File indexFile = new File(instanceDirectory, "testbag_field1field2.idx");

            Index index = indexManager.getIndex("testbag", "field1field2");
            assertNotNull("Index should have been loaded", index);
            assertEquals("Index instances should be the same", createdIndex.hashCode(), index.hashCode());

            index.flushIndex();
            assertTrue("There should be an index", indexFile.exists());

            KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(complexIndexField.getIndexFields()), Lists.newArrayList(new IndexField("payload", new StringKeyType()), new IndexField(SimpleEntity.DOCUMENT_ID, new UUIDKeyType())));
            IndexDefinition definition = new IndexDefinition(keyInfo.getKeyName(), keyInfo.keyAsHeader(), keyInfo.valueAsHeader(), index.getIndexType());

            verify(metadataStore, times(1)).addBagIndex(TEST_INSTANCE, TESTBAG, definition);
        } finally {
            indexManager.shutdownIndexes();
        }
    }

    @Test
    public void testBestIndexMatch() throws Exception {
        try {
            File instanceDirectory = temporaryFolder.newFolder();

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(new ArrayList<IndexDefinition>());
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            indexManager.createIndex(TESTBAG, new IndexField("field1", new StringKeyType(100)), false);
            indexManager.createIndex(TESTBAG, new IndexField("field2", new StringKeyType(100)), false);
            indexManager.createIndex(TESTBAG, new IndexField("field3", new StringKeyType(100)), false);

            assertThat(indexManager.getIndexes(TESTBAG).size(), is(3));

            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field1")), notNullValue());
            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field1")).getKeyInfo().getKeyName(), is("field1ID"));
            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field2")), notNullValue());
            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field2")).getKeyInfo().getKeyName(), is("field2ID"));
            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field3")), notNullValue());
            assertThat(indexManager.getBestMatchingIndex(TESTBAG, Sets.newHashSet("field3")).getKeyInfo().getKeyName(), is("field3ID"));
        } finally {
            indexManager.shutdownIndexes();
        }
    }

    @Test
    public void testBagIndexMultiKeyCreate() throws Exception {
        try {
            File instanceDirectory = temporaryFolder.newFolder();

            Bag bag = mock(Bag.class);
            when(bag.getIndexDefinitions()).thenReturn(new ArrayList<IndexDefinition>());
            when(metadataStore.getBag(TEST_INSTANCE, TESTBAG)).thenReturn(bag);

            Instance instance = mock(Instance.class);
            when(instance.getPath()).thenReturn(instanceDirectory.toString());
            when(metadataStore.getInstance(TEST_INSTANCE)).thenReturn(instance);

            indexManager.createIndex(TESTBAG, new IndexField("field1", new StringKeyType()), false);
            indexManager.createIndex(TESTBAG, new CompositeIndexField(new IndexField("field1", new StringKeyType()), new IndexField("field2", new StringKeyType())), false);

            Map<String, Index> indexes = indexManager.getIndexes(TESTBAG);
            assertThat(indexes.containsKey("field1ID"), is(true));
            assertThat(indexes.containsKey("field1field2ID"), is(true));
        } finally {
            indexManager.shutdownIndexes();
        }
    }
}
