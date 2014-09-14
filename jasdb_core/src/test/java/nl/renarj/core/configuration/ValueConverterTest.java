package nl.renarj.core.configuration;

import junit.framework.Assert;
import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import org.junit.Test;

public class ValueConverterTest {
	@Test
	public void testMemoryConversion() throws Exception {
		Assert.assertEquals(1024L, ValueConverterUtil.convertToBytes("1k"));
		Assert.assertEquals(10240L, ValueConverterUtil.convertToBytes("10k"));
		Assert.assertEquals(102400L, ValueConverterUtil.convertToBytes("100k"));
		Assert.assertEquals(1048576L, ValueConverterUtil.convertToBytes("1"));
		Assert.assertEquals(10485760L, ValueConverterUtil.convertToBytes("10"));
		Assert.assertEquals(104857600L, ValueConverterUtil.convertToBytes("100"));
		Assert.assertEquals(1048576L, ValueConverterUtil.convertToBytes("1m"));
		Assert.assertEquals(10485760L, ValueConverterUtil.convertToBytes("10m"));
		Assert.assertEquals(104857600L, ValueConverterUtil.convertToBytes("100m"));
		Assert.assertEquals(1073741824L, ValueConverterUtil.convertToBytes("1g"));
		Assert.assertEquals(10737418240L, ValueConverterUtil.convertToBytes("10g"));
		Assert.assertEquals(107374182400L, ValueConverterUtil.convertToBytes("100g"));
	}
	
	@Test
	public void testMemoryCapitalCasingConversion() throws Exception {
		Assert.assertEquals(1024L, ValueConverterUtil.convertToBytes("1K"));
		Assert.assertEquals(10240L, ValueConverterUtil.convertToBytes("10K"));
		Assert.assertEquals(102400L, ValueConverterUtil.convertToBytes("100K"));
		Assert.assertEquals(1048576L, ValueConverterUtil.convertToBytes("1M"));
		Assert.assertEquals(10485760L, ValueConverterUtil.convertToBytes("10M"));
		Assert.assertEquals(104857600L, ValueConverterUtil.convertToBytes("100M"));
		Assert.assertEquals(1073741824L, ValueConverterUtil.convertToBytes("1G"));
		Assert.assertEquals(10737418240L, ValueConverterUtil.convertToBytes("10G"));
		Assert.assertEquals(107374182400L, ValueConverterUtil.convertToBytes("100G"));
	}
	
	@Test
	public void testMemoryInvalidConversion() throws Exception {
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("1x"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("1     S"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("1 HK"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("1 P"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("H"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("M"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("S"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("s"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("d"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely("100mb"));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely(""));
		Assert.assertFalse("Invalid conversion, should give error", convertMemorySafely(null));
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
		Assert.assertEquals(1000L, ValueConverterUtil.convertToMilliseconds("1s"));
		Assert.assertEquals(10000L, ValueConverterUtil.convertToMilliseconds("10s"));
		Assert.assertEquals(100000L, ValueConverterUtil.convertToMilliseconds("100s"));
		Assert.assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1"));
		Assert.assertEquals("Expected 10 minute", 600000L, ValueConverterUtil.convertToMilliseconds("10"));
		Assert.assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100"));
		Assert.assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1m"));
		Assert.assertEquals("Expected 10 minutes", 600000L, ValueConverterUtil.convertToMilliseconds("10m"));
		Assert.assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100m"));
		Assert.assertEquals(3600000L, ValueConverterUtil.convertToMilliseconds("1h"));
		Assert.assertEquals(36000000L, ValueConverterUtil.convertToMilliseconds("10h"));
		Assert.assertEquals(360000000L, ValueConverterUtil.convertToMilliseconds("100h"));
		Assert.assertEquals(86400000L, ValueConverterUtil.convertToMilliseconds("1d"));
		Assert.assertEquals(864000000L, ValueConverterUtil.convertToMilliseconds("10d"));
		Assert.assertEquals(8640000000L, ValueConverterUtil.convertToMilliseconds("100d"));
	}
	
	@Test
	public void testTimeConversionCapitalCasing() throws Exception {
		Assert.assertEquals(1000L, ValueConverterUtil.convertToMilliseconds("1S"));
		Assert.assertEquals(10000L, ValueConverterUtil.convertToMilliseconds("10S"));
		Assert.assertEquals(100000L, ValueConverterUtil.convertToMilliseconds("100S"));
		Assert.assertEquals("Expected 1 minute", 60000L, ValueConverterUtil.convertToMilliseconds("1M"));
		Assert.assertEquals("Expected 10 minutes", 600000L, ValueConverterUtil.convertToMilliseconds("10M"));
		Assert.assertEquals("Expected 100 minute", 6000000L, ValueConverterUtil.convertToMilliseconds("100M"));
		Assert.assertEquals(3600000L, ValueConverterUtil.convertToMilliseconds("1H"));
		Assert.assertEquals(36000000L, ValueConverterUtil.convertToMilliseconds("10H"));
		Assert.assertEquals(360000000L, ValueConverterUtil.convertToMilliseconds("100H"));
		Assert.assertEquals(86400000L, ValueConverterUtil.convertToMilliseconds("1D"));
		Assert.assertEquals(864000000L, ValueConverterUtil.convertToMilliseconds("10D"));
		Assert.assertEquals(8640000000L, ValueConverterUtil.convertToMilliseconds("100D"));
	}
	
	@Test
	public void testInvalidTimeConversion() throws Exception {
		Assert.assertFalse(convertTimeSafely("1k"));
		Assert.assertFalse(convertTimeSafely("1x"));
		Assert.assertFalse(convertTimeSafely("1     S"));
		Assert.assertFalse(convertTimeSafely("1 HK"));
		Assert.assertFalse(convertTimeSafely("1 P"));
		Assert.assertFalse(convertTimeSafely("H"));
		Assert.assertFalse(convertTimeSafely("M"));
		Assert.assertFalse(convertTimeSafely("S"));
		Assert.assertFalse(convertTimeSafely("s"));
		Assert.assertFalse(convertTimeSafely("d"));
		Assert.assertFalse(convertTimeSafely("100mb"));
		Assert.assertFalse(convertTimeSafely(""));
		Assert.assertFalse(convertTimeSafely(null));
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
