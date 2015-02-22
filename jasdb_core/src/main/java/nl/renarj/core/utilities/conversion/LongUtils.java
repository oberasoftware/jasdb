package nl.renarj.core.utilities.conversion;

/**
 * @author renarj
 */
public final class LongUtils {

    private static final int BITMASK = 0xff;
    private static final int LONG_SIZE = 8;

    private static final int SHIFT_SIZE = 8;

    private LongUtils() {}

    public static long bytesToLong(byte[] bytes) {
        long v = 0;

        if(bytes != null && bytes.length == LONG_SIZE) {
            for (int i = LONG_SIZE - 1; i >= 0; i--) {
                v |= (long) bytes[i] & BITMASK;
                if (i != 0) {
                    v <<= SHIFT_SIZE;
                }
            }
        }

        return v;
    }

    public static byte[] longToBytes(long value) {
        byte[] buffer = new byte[LONG_SIZE];
        for(int i=0; i< LONG_SIZE; i++) {
            buffer[i] = (byte) value;
            value = value >> SHIFT_SIZE;
        }

        return buffer;
    }
}
