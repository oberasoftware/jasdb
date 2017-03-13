package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockHeader;
import com.oberasoftware.jasdb.api.storage.DataBlockResult;
import com.oberasoftware.jasdb.api.index.IndexHeader;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Renze de Vries
 */
public class BtreeIndexHeader implements IndexHeader {
    public static final int BTREE_INDEX_TYPE_ID = 2;

    private static final int INDEX_VERSION = 4;

    private static final int[] supportedVersions = {4};


    private static final int TYPE_HEADER_INDEX = 0;
    private static final int VERSION_HEADER_INDEX = 4;
    private static final int PAGE_SIZE_INDEX = 8;
    private static final int COUNT_INDEX = 12;


    private KeyInfo keyInfo;
    private int indexVersion;
    private int pageSize;
    private int headerSize;
    private long count;

    public BtreeIndexHeader(int indexVersion, int pageSize, int headerSize, KeyInfo keyInfo, long count) {
        this.keyInfo = keyInfo;
        this.pageSize = pageSize;
        this.headerSize = headerSize;
        this.indexVersion = indexVersion;
        this.count = count;
    }

    public static IndexHeader loadAndValidateHeader(DataBlock dataBlock, KeyInfo keyInfo) throws JasDBStorageException, IOException {
        DataBlockHeader dataBlockHeader = dataBlock.getHeader();
        int version = dataBlockHeader.getInt(VERSION_HEADER_INDEX);
        int type = dataBlockHeader.getInt(TYPE_HEADER_INDEX);
        int pageSize = dataBlockHeader.getInt(PAGE_SIZE_INDEX);
        long count = dataBlockHeader.getLong(COUNT_INDEX);

        DataBlockResult<byte[]> keyResult = dataBlock.loadBytes(0);
        DataBlockResult<byte[]> keyValueResult = dataBlock.loadBytes(keyResult.getNextOffset());

        if(Arrays.binarySearch(supportedVersions, version) == -1) {
            throw new JasDBStorageException("Version of found index is not supported by this version");
        } else if(type != BTREE_INDEX_TYPE_ID) {
            throw new JasDBStorageException("Index type is not supported or invalid");
        } else if(!keyInfo.keyAsHeader().equals(new String(keyResult.getValue()))) {
            throw new JasDBStorageException("Key information in index does not match specification on the storage metadata");
        } else if(!keyInfo.valueAsHeader().equals(new String(keyValueResult.getValue()))) {
            throw new JasDBStorageException("Key information in index does not match specification on the storage metadata");
        } else {
            return new BtreeIndexHeader(version, pageSize, dataBlock.size(), keyInfo, count);
        }
    }

    public static IndexHeader createHeader(DataBlock headerBlock, int pageSize, long count, KeyInfo keyInfo) throws JasDBStorageException, IOException {
        String headerInfo = keyInfo.keyAsHeader();
        byte[] headerInfoBytes = headerInfo.getBytes();

        String valueHeaderInfo = keyInfo.valueAsHeader();
        byte[] valueHeaderInfoBytes = valueHeaderInfo.getBytes();

        DataBlockHeader dataBlockHeader = headerBlock.getHeader();
        dataBlockHeader.putInt(VERSION_HEADER_INDEX, INDEX_VERSION);
        dataBlockHeader.putInt(TYPE_HEADER_INDEX, BTREE_INDEX_TYPE_ID);
        dataBlockHeader.putInt(PAGE_SIZE_INDEX, pageSize);
        dataBlockHeader.putLong(COUNT_INDEX, count);
        headerBlock.reset();
        headerBlock.writeBytes(headerInfoBytes);
        headerBlock.writeBytes(valueHeaderInfoBytes);

        headerBlock.flush();

        return new BtreeIndexHeader(INDEX_VERSION, pageSize, headerBlock.size(), keyInfo, count);
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public KeyInfo getKeyInfo() {
        return keyInfo;
    }

    @Override
    public int getIndexVersion() {
        return indexVersion;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getHeaderSize() {
        return headerSize;
    }
}
