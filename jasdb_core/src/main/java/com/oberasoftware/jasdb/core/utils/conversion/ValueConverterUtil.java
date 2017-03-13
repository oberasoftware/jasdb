package com.oberasoftware.jasdb.core.utils.conversion;

import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.core.utils.StringUtils;

public class ValueConverterUtil {
	public static long convertToBytes(String memorySize) throws CoreConfigException {
		return convertUnit(memorySize, new MemoryTypeConverter());
	}
	
	public static long convertToBytes(String memorySize, long defaultValue) throws CoreConfigException {
		try {
			return convertUnit(memorySize, new MemoryTypeConverter());
		} catch(CoreConfigException e) {
			return defaultValue;
		}
	}
	
	public static long convertToMilliseconds(String timeSpan) throws CoreConfigException {
		return convertUnit(timeSpan, new TimeTypeConverter());
	}
	
	public static long convertToMilliseconds(String timeSpan, long defaultValue) throws CoreConfigException {
		try {
			return convertUnit(timeSpan, new TimeTypeConverter());
		} catch(CoreConfigException e) {
			return defaultValue;
		}
	}
	
	private static long convertUnit(String unit, ValueConverterType converter) throws CoreConfigException {
		if(StringUtils.stringNotEmpty(unit)) {
			try {
				char lastDigit = unit.charAt(unit.length() - 1);
				lastDigit = Character.toLowerCase(lastDigit);
				
				long longUnit = -1;
				if(Character.isDigit(lastDigit)) {
					longUnit = Long.valueOf(unit);
				} else {
					String value = unit.substring(0, unit.length() - 1);
					longUnit = Long.valueOf(value);
				}

				return converter.convertToLong(lastDigit, longUnit);
			} catch(NumberFormatException e) {
				throw new CoreConfigException("Unable to parse value in unit: " + unit, e);
			}
		} else {
			throw new CoreConfigException("Could not parse empty unit");
		}
	}
	
	public static int safeConvertInteger(String integer, int defaultValue) {
		try {
			return Integer.valueOf(integer);
		} catch(NumberFormatException e) {
			return defaultValue;
		}
	}
}
