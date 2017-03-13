package com.oberasoftware.jasdb.core.utils.conversion;

import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;

public class TimeTypeConverter implements ValueConverterType {

	@Override
	public long convertToLong(final char type, final long value) throws CoreConfigException {
		long unit = value;
		switch(type) {
			case 'd':
				unit = unit * 24;
			case 'h':
				unit = unit * 60;
			case 'm':
				unit = unit * 60;
			case 's':
				unit = unit * 1000;
				return unit;
			default :
				if(Character.isDigit(type)) {
					return unit * 1000 * 60;
				} else {
					throw new CoreConfigException("Unrecognized quanitifier on time unit: " + type);
				}
		}
	}

}
