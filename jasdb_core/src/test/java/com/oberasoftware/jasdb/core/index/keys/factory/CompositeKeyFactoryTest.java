package com.oberasoftware.jasdb.core.index.keys.factory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyNameMapperImpl;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.MultiKeyLoaderImpl;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.MultiKeyloader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public class CompositeKeyFactoryTest {
    @Test
    public void testCreateMultiValueKeys() throws JasDBStorageException {
        KeyNameMapper nameMapper = new KeyNameMapperImpl();
        nameMapper.addMappedField("city");
        nameMapper.addMappedField("__ID");
        nameMapper.setValueMarker(2);
        KeyFactory cityKeyFactory = new StringKeyFactory("city", "1000");
        KeyFactory idKeyFactory = new UUIDKeyFactory("__ID");

        MultiKeyloader multiKeyloader = new MultiKeyLoaderImpl(nameMapper, new KeyFactory[]{cityKeyFactory, idKeyFactory});
        CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory(multiKeyloader);

        String randomId = UUID.randomUUID().toString();
        Map<String, List<String>> properties = Maps.newHashMap();
        properties.put("city", Lists.newArrayList("Amsterdam", "Rotterdam"));
        properties.put("__ID", Lists.newArrayList(randomId));

        Set<Key> keys = compositeKeyFactory.createMultivalueKeys(new MockedIndexableItem(properties));
        assertEquals(2, keys.size());

        Key expectedCompositeKey1 = new CompositeKey();
        expectedCompositeKey1.addKey(nameMapper, "city", new StringKey("Amsterdam"));
        expectedCompositeKey1.addKey(nameMapper, "__ID", new UUIDKey(randomId));

        Key expectedCompositeKey2 = new CompositeKey();
        expectedCompositeKey2.addKey(nameMapper, "city", new StringKey("Rotterdam"));
        expectedCompositeKey2.addKey(nameMapper, "__ID", new UUIDKey(randomId));

        assertThat(keys, hasItems(expectedCompositeKey1, expectedCompositeKey2));
    }

    private class MockedIndexableItem implements IndexableItem {
        private Map<String, List<String>> properties = new HashMap<>();

        private MockedIndexableItem(Map<String, List<String>> properties) {
            this.properties = properties;
        }

        @Override
        public boolean hasValue(String propertyName) {
            return properties.containsKey(propertyName);
        }

        @Override
        public Object getValue(String propertyName) {
            return properties.get(propertyName).get(0);
        }

        @Override
        public List<Object> getValues(String propertyName) {
            return new ArrayList<Object>(properties.get(propertyName));
        }

        @Override
        public boolean isMultiValue(String propertyName) {
            return properties.get(propertyName).size() > 1;
        }
    }
}
