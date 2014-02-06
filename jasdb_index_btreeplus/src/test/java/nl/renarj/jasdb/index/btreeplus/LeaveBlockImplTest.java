package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.btreeplus.persistence.BtreePlusBlockPersister;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapperImpl;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class LeaveBlockImplTest {
    private static final int NR_AGES = 100;
    private static final int NRKEYS = 3;

    private BtreePlusBlockPersister persister;
    private DataBlock dataBlock;

    public LeaveBlockImplTest() {
        dataBlock = mock(DataBlock.class);
        persister = mock(BtreePlusBlockPersister.class);
        when(persister.getMaxKeys()).thenReturn(512);
    }

    @Test
    public void testCompositeKeyRangeTest() throws JasDBStorageException {
        LeaveBlock leaveBlock = createBlock(NR_AGES, NRKEYS);

        KeyNameMapper mapper = createMapper("age", "magicnr");
        for(int i=0; i< NR_AGES; i++) {
            Key findKey = new CompositeKey().addKey(mapper, "age", new LongKey(i));

            List<Key> resultKeys = leaveBlock.getKeyRange(findKey, true, findKey, true);
            assertEquals(NRKEYS, resultKeys.size());
        }
    }

    @Test
    public void testCompositeKeyRangeTestKeyConversion() throws JasDBStorageException {
        LeaveBlock leaveBlock = createBlock(NR_AGES, NRKEYS);

        KeyNameMapper mapper = createMapper("age", "magicnr");
        for(int i=0; i< NR_AGES; i++) {
            Key findKey = new CompositeKey().addKey(mapper, "age", new StringKey(String.valueOf(i)));
//            StringKey ageKey = new StringKey(String.valueOf(i));
            List<Key> resultKeys = leaveBlock.getKeyRange(findKey, true, findKey, true);
            assertEquals(NRKEYS, resultKeys.size());
        }
    }

    private KeyNameMapper createMapper(String... fields) {
        KeyNameMapper mapper = new KeyNameMapperImpl();
        for(String field : fields) {
            mapper.addMappedField(field);
        }
        mapper.setValueMarker(fields.length);
        return mapper;
    }

    private LeaveBlock createBlock(int nrAges, int nrkeys) throws JasDBStorageException {
        KeyNameMapper mapper = createMapper("age", "magicnr");
        LeaveBlockImpl leaveBlock = new LeaveBlockImpl(persister, dataBlock, -1, false);

        for(int i=0; i<nrAges; i++) {
            for(int key=0; key<nrkeys; key++) {
                leaveBlock.insertKey(new CompositeKey()
                        .addKey(mapper, "age", new LongKey(i))
                        .addKey(mapper, "magicnr", new LongKey(key)));
            }
        }

        return leaveBlock;
    }
}
