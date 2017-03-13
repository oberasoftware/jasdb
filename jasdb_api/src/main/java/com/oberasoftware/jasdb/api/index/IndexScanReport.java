package com.oberasoftware.jasdb.api.index;

/**
 * @author renarj
 */
public interface IndexScanReport {
    IndexState getState();

    long getLastScan();

    int getCompleteness();
}
