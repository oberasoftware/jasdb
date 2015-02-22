package nl.renarj.jasdb.index;

/**
 * @author Renze de Vries
 */
public enum IndexState {
    NOT_INITIALIZED,
    OK,
    REBUILDING,
    INVALID,
    CLOSED
}
