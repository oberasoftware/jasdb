package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
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
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("category", new StringKeyType(200)), new SimpleIndexField("RECORD_POINTER", new LongKeyType()));
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
            index.close();
        }

        index = new BTreeIndex(new File(tmpDir, "category.idx"), keyInfo);
        try {
            assertIndex(categories, index);
        } finally {
            index.close();
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


        Set<String> categories = new HashSet<>();
        String category;
        while((category = bufferedReader.readLine()) != null) {
            categories.add(category.trim().toLowerCase());
        }

        return categories;
    }
}
