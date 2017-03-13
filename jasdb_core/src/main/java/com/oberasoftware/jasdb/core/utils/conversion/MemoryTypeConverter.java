package com.oberasoftware.jasdb.core.utils.conversion;

import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;

public class MemoryTypeConverter implements ValueConverterType {

	@Override
	public long convertToLong(final char type, final long value) throws CoreConfigException {
		long unit = value;
		switch(type) {
			case 'g':
				unit = unit * 1024;
			case 'm':
				unit = unit * 1024;
			case 'k':
				unit = unit * 1024;
				return unit;
			default :
				if(Character.isDigit(type)) {
					return unit * 1024 * 1024;
				} else {
					throw new CoreConfigException("Unrecognized quanitifier on memory spec: " + type);
				}
		}
	}

}
