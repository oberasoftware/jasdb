/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.MetadataParseException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.core.storage.ClonableByteArrayInputStream;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyNameMapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple utility class that does Bag entity operations
 *
 * @author Renze de Vries
 */
public class BagOperationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BagOperationUtil.class);

    private static final String ENTITY_ENCODING = "UTF8";

    public static KeyNameMapperImpl DEFAULT_DOC_ID_MAPPER = new KeyNameMapperImpl();
    static {
        DEFAULT_DOC_ID_MAPPER.addMappedField(0, SimpleEntity.DOCUMENT_ID);
    }

    public static ClonableDataStream toStream(Entity entity) throws JasDBStorageException {
        try {
            String entityJson = SimpleEntity.toJson(entity);

            return new ClonableByteArrayInputStream(entityJson.getBytes(ENTITY_ENCODING));
        } catch(MetadataParseException e) {
            throw new JasDBStorageException("Unable to serialize to json", e);
        } catch(UnsupportedEncodingException e) {
            throw new JasDBStorageException("Unable to encode entity into Unicode", e);
        }
    }

    public static Entity toEntity(InputStream stream) throws JasDBStorageException {
        return SimpleEntity.fromStream(stream);
    }

    public static Set<Key> createEntityKeys(Entity entity, Index index) throws JasDBStorageException {
        StatRecord createKey = StatisticsMonitor.createRecord("bag:createKey");
        KeyFactory keyFactory = index.getKeyInfo().getKeyFactory();
        Set<Key> insertKeys;
        if(keyFactory.isMultiValueKey(entity)) {
            insertKeys = keyFactory.createMultivalueKeys(entity);
        } else {
            insertKeys = new HashSet<>();
            insertKeys.add(keyFactory.createKey(entity));
        }
        createKey.stop();
        return insertKeys;
    }
    
    public static void doIndexInsert(Set<Key> keys, Index index) throws JasDBStorageException {
        LOG.trace("Inserting {} keys into index: {}", keys.size(), index.getKeyInfo().getKeyName());
        StatRecord indexInsert = StatisticsMonitor.createRecord("bag:indexInsert");
        for(Key key : keys) {
            index.insertIntoIndex(key);
        }
        indexInsert.stop();
    }

    public static Key recordToKey(RecordResult recordResult) {
        try {
            Entity entity = toEntity(recordResult.getStream());
            return entityToKey(entity);
        } catch (JasDBStorageException e) {
            throw new RuntimeJasDBException("Unable to read record Document Id", e);
        }
    }

    public static Key entityToKey(Entity entity) throws JasDBStorageException {
        CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(DEFAULT_DOC_ID_MAPPER, SimpleEntity.DOCUMENT_ID, new UUIDKey(entity.getInternalId()));

        return compositeKey;
    }
}
