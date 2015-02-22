package nl.renarj.core.utilities.conversion;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author renarj
 */
public class LongUtilsTest {
    @Test
    public void testConvertAndBack() {
        long original = System.currentTimeMillis();

        byte[] converted = LongUtils.longToBytes(original);

        assertThat(LongUtils.bytesToLong(converted), is(original));
    }

    @Test
    public void testEmptyArray() {
        long result = LongUtils.bytesToLong(new byte[0]);
        assertThat(result, is(0l));
    }
}
