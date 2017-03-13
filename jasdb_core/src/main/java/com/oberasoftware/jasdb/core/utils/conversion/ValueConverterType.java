package com.oberasoftware.jasdb.core.utils.conversion;

import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;

public interface ValueConverterType {
	long convertToLong(char type, long value) throws CoreConfigException;
}
