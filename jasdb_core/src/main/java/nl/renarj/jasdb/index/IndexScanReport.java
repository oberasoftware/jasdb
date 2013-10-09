package nl.renarj.jasdb.index;

/**
 * @author Renze de Vries
 */
public class IndexScanReport {
    private IndexState state;
    private long lastScan;
    private int completeness;

    public IndexScanReport(IndexState state, long lastScan, int completeness) {
        this.state = state;
        this.lastScan = lastScan;
        this.completeness = completeness;
    }

    public IndexState getState() {
        return state;
    }

    public long getLastScan() {
        return lastScan;
    }

    public int getCompleteness() {
        return completeness;
    }
}
