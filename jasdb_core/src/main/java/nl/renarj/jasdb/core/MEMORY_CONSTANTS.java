package nl.renarj.jasdb.core;

/**
 * @author Renze de Vries
 */
public interface MEMORY_CONSTANTS {
    static final int INTEGER_BYTE_SIZE = Integer.SIZE / Byte.SIZE;

    static final int LONG_BYTE_SIZE = (Long.SIZE / Byte.SIZE);

    static final int TWO_LONG_BYTES = LONG_BYTE_SIZE + LONG_BYTE_SIZE;

    static final int OBJECT_REF = 8;

    static final int OBJECT_SIZE = 16;

    static final int ARRAY_SIZE = 12;
}
