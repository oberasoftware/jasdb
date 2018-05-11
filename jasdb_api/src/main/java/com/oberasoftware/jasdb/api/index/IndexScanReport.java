package com.oberasoftware.jasdb.api.index;

/**
 * @author Renze de Vries
 */
public interface IndexScanReport {
    IndexState getState();

    long getLastScan();

    int getCompleteness();
}
