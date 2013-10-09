package nl.renarj.jasdb.index;

import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;

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
