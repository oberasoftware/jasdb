package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.index.keys.CompareMethod;
import com.oberasoftware.jasdb.core.index.keys.AnyKey;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyNameMapperImpl;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
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
