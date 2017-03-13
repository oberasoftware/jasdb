package com.oberasoftware.jasdb.core.index;

import com.oberasoftware.jasdb.api.index.IndexScanReport;
import com.oberasoftware.jasdb.api.index.IndexState;

/**
 * @author Renze de Vries
 */
public class IndexScanReportImpl implements IndexScanReport {
    private IndexState state;
    private long lastScan;
    private int completeness;

    public IndexScanReportImpl(IndexState state, long lastScan, int completeness) {
        this.state = state;
        this.lastScan = lastScan;
        this.completeness = completeness;
    }

    @Override
    public IndexState getState() {
        return state;
    }

    @Override
    public long getLastScan() {
        return lastScan;
    }

    @Override
    public int getCompleteness() {
        return completeness;
    }
}
