package com.oberasoftware.jasdb.api.index.keys;

import com.oberasoftware.jasdb.api.storage.DataBlock;

/**
 * @author Renze de Vries
 */
public interface KeyLoadResult {
    Key getLoadedKey();

    DataBlock getEndBlock();

    int getNextOffset();
}
