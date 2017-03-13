package com.oberasoftware.jasdb.api.index;

/**
 * @author Renze de Vries
 */
public interface MemoryConstants {
    int LONG_BYTE_SIZE = Long.SIZE / Byte.SIZE;

    int OBJECT_BYTE_SIZE = 16;

    int ARRAY_BYTE_SIZE = 12;
    int INTEGER_BYTE_SIZE = Integer.SIZE / Byte.SIZE;

    int TWO_LONG_BYTES = LONG_BYTE_SIZE + LONG_BYTE_SIZE;
    int OBJECT_REF = 8;
    int OBJECT_SIZE = 16;
    int ARRAY_SIZE = 12;
}
