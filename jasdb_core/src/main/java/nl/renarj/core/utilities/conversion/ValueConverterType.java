package nl.renarj.core.utilities.conversion;

import nl.renarj.core.exceptions.CoreConfigException;

public interface ValueConverterType {
	long convertToLong(char type, long value) throws CoreConfigException;
}
