package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.impl.AnyKey;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapperImpl;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author renarj
 */
public class AnyKeyTest {
    @Test
    public void testCompareStringNillKey() {
        assertThat(new AnyKey().compare(new StringKey((byte[]) null), CompareMethod.EQUALS).getCompare(), is(0));

        assertThat(new StringKey((byte[]) null).compare(new AnyKey(), CompareMethod.EQUALS).getCompare(), is(-1));
    }

    @Test
    public void testCompareStringKey() {
        assertThat(new AnyKey().compare(new StringKey("key"), CompareMethod.EQUALS).getCompare(), is(0));

        assertThat(new StringKey("key").compare(new AnyKey(), CompareMethod.EQUALS).getCompare(), is(-1));
    }

    @Test
    public void testCompareLongKey() {

    }

    @Test
    public void testCompareNillLongKey() {
        assertThat(new AnyKey().compare(new LongKey(new byte[0]), CompareMethod.EQUALS).getCompare(), is(0));

        assertThat(new LongKey(new byte[0]).compare(new AnyKey(), CompareMethod.EQUALS).getCompare(), is(-1));
    }

    @Test
    public void testCompareCompositeKey() {
        KeyNameMapper km = new KeyNameMapperImpl();
        km.addMappedField("field1");
        km.addMappedField("field2");
        km.addMappedField("field3");
        km.setValueMarker(3);

        CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(km, "field1", new StringKey("key"))
            .addKey(km, "field2", new LongKey(10l))
            .addKey(km, "field3", new UUIDKey(UUID.randomUUID()));


        CompositeKey anyKey = new CompositeKey();
        anyKey.addKey(km, "field1", new AnyKey())
            .addKey(km, "field2", new AnyKey())
            .addKey(km, "field3", new AnyKey());

        assertThat(anyKey.compare(compositeKey, CompareMethod.EQUALS).getCompare(), is(0));
    }
}
