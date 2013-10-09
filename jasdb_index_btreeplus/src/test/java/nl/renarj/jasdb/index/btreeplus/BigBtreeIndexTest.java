package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;

/**
 * @author Renze de Vries
 */
public class BigBtreeIndexTest extends IndexBaseTest {
    private Logger log = LoggerFactory.getLogger(BigBtreeIndexTest.class);

    @Before
    public void setUp() {
        cleanData();
    }

    @After
    public void tearDown() {
        cleanData();
    }

    @Test
    public void testInsertCategories() throws JasDBStorageException, IOException, InterruptedException {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("category", new StringKeyType(200)), new IndexField("RECORD_POINTER", new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "category.idx"), keyInfo);

        Set<String> categories = getCategories();
        log.info("Loaded: {} categories", categories.size());

        try {
            long recordCounter = 0;
            for(String category : categories) {
                index.insertIntoIndex(new StringKey(category).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(recordCounter)));
                recordCounter++;
            }

            assertIndex(categories, index);
        } finally {
            index.closeIndex();
        }

        index = new BTreeIndex(new File(tmpDir, "category.idx"), keyInfo);
        try {
            assertIndex(categories, index);
        } finally {
            index.closeIndex();
        }

    }

    private void assertIndex(Set<String> categories, Index index) throws JasDBStorageException {
        for(String category : categories) {
            assertFalse("There should be a key found for category: " + category, index.searchIndex(new EqualsCondition(new StringKey(category)), Index.NO_SEARCH_LIMIT).isEmpty());
        }
    }

    private Set<String> getCategories() throws IOException {
        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("categories.txt");
        InputStreamReader reader = new InputStreamReader(is);
        BufferedReader bufferedReader = new BufferedReader(reader);


        Set<String> categories = new HashSet<String>();
        String category;
        while((category = bufferedReader.readLine()) != null) {
            categories.add(category.trim().toLowerCase());
        }

        return categories;
    }
}
