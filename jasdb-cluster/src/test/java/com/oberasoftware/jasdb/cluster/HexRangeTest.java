package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.cluster.model.HexRange;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public class HexRangeTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void splitRange() {
        List<HexRange> ranges = HexRange.DEFAULT.splitHexRange();
        assertThat(ranges, hasItems(new HexRange("0", "7"), new HexRange("8", "F")));
    }

    @Test
    public void splitRanges() {
        List<HexRange> ranges = HexRange.generate(8);
        assertThat(ranges.size(), is(8));

        assertThat(ranges, hasItems(
                range("0", "1"),
                range("2", "3"),
                range("4", "5"),
                range("6", "7"),
                range("8", "9"),
                range("A", "B"),
                range("C", "D"),
                range("E", "F")
        ));
    }

    @Test
    public void splitRangesBigSize() {
        List<HexRange> ranges = HexRange.generate(1024);
        assertThat(ranges.size(), is(1024));

        assertThat(ranges.get(0), is(range("000", "003")));
        assertThat(ranges.get(1023), is(range("FFC", "FFF")));
    }

    @Test
    public void splitRangesInvalidPowerOf2() {
        expectedException.expect(IllegalArgumentException.class);
        HexRange.generate(7);
    }

    private HexRange range(String start, String end) {
        return new HexRange(start, end);
    }

}
