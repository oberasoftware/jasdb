package com.oberasoftware.jasdb.api.index.keys;

import com.oberasoftware.jasdb.api.storage.DataBlock;

/**
 * @author renarj
 */
public interface KeyLoadResult {
    Key getLoadedKey();

    DataBlock getEndBlock();

    int getNextOffset();
}
