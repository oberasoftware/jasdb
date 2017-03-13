package com.oberasoftware.jasdb.api.index;

import com.oberasoftware.jasdb.api.index.keys.KeyInfo;

/**
 * @author Renze de Vries
 */
public interface IndexHeader {
    int getPageSize();

    KeyInfo getKeyInfo();

    int getIndexVersion();

    int getHeaderSize();

    long count();
}
