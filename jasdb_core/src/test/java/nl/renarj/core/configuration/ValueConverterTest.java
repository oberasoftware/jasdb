package nl.renarj.core.configuration;

import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ValueConverterTest {
	@Test
	public void testMemoryConversion() throws Exception {
		assertEquals(1024L, ValueConverterUtil.convertToBytes("1k"));
		assertEquals(10240L, ValueConverterUtil.convertToBytes("10k"));
		assertEquals(102400L, ValueConverterUtil.convertToBytes("100k"));
		assertEquals(1048576L, ValueConverterUtil.convertToBytes("1"));
		assertEquals(10485760L, ValueConverterUtil.convertToBytes("10"));
		assertEquals(104857600L, ValueConverterUtil.convertToBytes("100"));
		assertEquals(1048576L, ValueConverterUtil.convertToBytes("1m"));
		assertEquals(10485760L, ValueConverterUtil.convertToBytes("10m"));
		assertEquals(104857600L, ValueConverterUtil.convertToBytes("100m"));
		assertEquals(1073741824L, ValueConverterUtil.convertToBytes("1g"));
		assertEquals(10737418240L, ValueConverterUtil.convertToBytes("10g"));
		assertEquals(107374182400L, ValueConverterUtil.convertToBytes("100g"));
	}
	
	@Test
	public void testMemoryCapitalCasingConversion() throws Exception {
		assertEquals(1024L, ValueConverterUtil.convertToBytes("1K"));
		assertEquals(10240L, ValueConverterUtil.convertToBytes("10K"));
		assertEquals(102400L, ValueConverterUtil.convertToBytes("100K"));
		assertEquals(1048576L, ValueConverterUtil.convertToBytes("1M"));
		assertEquals(10485760L, ValueConverterUtil.convertToBytes("10M"));
		assertEquals(104857600L, ValueConverterUtil.convertToBytes("100M"));
		assertEquals(1073741824L, ValueConverterUtil.convertToBytes("1G"));
		assertEquals(10737418240L, ValueConverterUtil.convertToBytes("10G"));
		assertEquals(107374182400L, ValueConverterUtil.convertToBytes("100G"));
	}
	
	@Test
	public void testMemoryInvalidConversion() throws Exception {
		assertFalse("Invalid conversion, should give error", convertMemorySafely("1x"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("1     S"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("1 HK"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("1 P"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("H"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("M"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("S"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("s"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("d"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely("100mb"));
		assertFalse("Invalid conversion, should give error", convertMemorySafely(""));
		assertFalse("Invalid conversion, should give error", convertMemorySafely(null));
	}
	
	@Test
	public void testMemoryInvalidConversionDefaults() throws Exception {
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("1x", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("1     S", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("1 HK", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("1 P", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("H", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("M", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("S", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("s", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("d", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("100mb", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely("", 1000));
		Assert.assertTrue("Invalid conversion, should not give error", convertMemorySafely(null, 1000));
	}
	
	@Test
	public void testTimeConversion() throws Exception {
		assertEquals(1000L, ValueConverterUtil.convertToMilliseconds("1s"));
		assertEquals(10000L, ValueConverterUtil.convertToMilliseconds("10s"));
		assertEquals(100000L, ValueConverterUtil.convertToMilliseconds("100s"));
		assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1"));
		assertEquals("Expected 10 minute", 600000L, ValueConverterUtil.convertToMilliseconds("10"));
		assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100"));
		assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1m"));
		assertEquals("Expected 10 minutes", 600000L, ValueConverterUtil.convertToMilliseconds("10m"));
		assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100m"));
		assertEquals(3600000L, ValueConverterUtil.convertToMilliseconds("1h"));
		assertEquals(36000000L, ValueConverterUtil.convertToMilliseconds("10h"));
		assertEquals(360000000L, ValueConverterUtil.convertToMilliseconds("100h"));
		assertEquals(86400000L, ValueConverterUtil.convertToMilliseconds("1d"));
		assertEquals(864000000L, ValueConverterUtil.convertToMilliseconds("10d"));
		assertEquals(8640000000L, ValueConverterUtil.convertToMilliseconds("100d"));
	}
	
	@Test
	public void testTimeConversionCapitalCasing() throws Exception {
		assertEquals(1000L, ValueConverterUtil.convertToMilliseconds("1S"));
		assertEquals(10000L, ValueConverterUtil.convertToMilliseconds("10S"));
		assertEquals(100000L, ValueConverterUtil.convertToMilliseconds("100S"));
		assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1M"));
		assertEquals("Expected 10 minutes", 600000L, ValueConverterUtil.convertToMilliseconds("10M"));
		assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100M"));
		assertEquals(3600000L, ValueConverterUtil.convertToMilliseconds("1H"));
		assertEquals(36000000L, ValueConverterUtil.convertToMilliseconds("10H"));
		assertEquals(360000000L, ValueConverterUtil.convertToMilliseconds("100H"));
		assertEquals(86400000L, ValueConverterUtil.convertToMilliseconds("1D"));
		assertEquals(864000000L, ValueConverterUtil.convertToMilliseconds("10D"));
		assertEquals(8640000000L, ValueConverterUtil.convertToMilliseconds("100D"));
	}
	
	@Test
	public void testInvalidTimeConversion() throws Exception {
		assertFalse(convertTimeSafely("1k"));
		assertFalse(convertTimeSafely("1x"));
		assertFalse(convertTimeSafely("1     S"));
		assertFalse(convertTimeSafely("1 HK"));
		assertFalse(convertTimeSafely("1 P"));
		assertFalse(convertTimeSafely("H"));
		assertFalse(convertTimeSafely("M"));
		assertFalse(convertTimeSafely("S"));
		assertFalse(convertTimeSafely("s"));
		assertFalse(convertTimeSafely("d"));
		assertFalse(convertTimeSafely("100mb"));
		assertFalse(convertTimeSafely(""));
		assertFalse(convertTimeSafely(null));
	}
	
	@Test
	public void testInvalidTimeConversionDefaults() throws Exception {
		Assert.assertTrue(convertTimeSafely("1k", 1000));
		Assert.assertTrue(convertTimeSafely("1x", 1000));
		Assert.assertTrue(convertTimeSafely("1     S", 1000));
		Assert.assertTrue(convertTimeSafely("1 HK", 1000));
		Assert.assertTrue(convertTimeSafely("1 P", 1000));
		Assert.assertTrue(convertTimeSafely("H", 1000));
		Assert.assertTrue(convertTimeSafely("M", 1000));
		Assert.assertTrue(convertTimeSafely("S", 1000));
		Assert.assertTrue(convertTimeSafely("s", 1000));
		Assert.assertTrue(convertTimeSafely("d", 1000));
		Assert.assertTrue(convertTimeSafely("100mb", 1000));
		Assert.assertTrue(convertTimeSafely("", 1000));
		Assert.assertTrue(convertTimeSafely(null, 1000));
	}
	
	public boolean convertTimeSafely(String timeSpan) {
		try {
			ValueConverterUtil.convertToMilliseconds(timeSpan);
			return true;
		} catch(CoreConfigException e) {
			return false;
		}
	}
	
	public boolean convertTimeSafely(String timeSpan, long defaultValue) {
		try {
			ValueConverterUtil.convertToMilliseconds(timeSpan, defaultValue);
			return true;
		} catch(CoreConfigException e) {
			return false;
		}
	}
	
	public boolean convertMemorySafely(String memorySize) {
		try {
			ValueConverterUtil.convertToBytes(memorySize);
			return true;
		} catch(CoreConfigException e) {
			return false;
		}
	}
	
	public boolean convertMemorySafely(String memorySize, long defaultValue) {
		try {
			ValueConverterUtil.convertToBytes(memorySize, defaultValue);
			return true;
		} catch(CoreConfigException e) {
			return false;
		}
	}
}
