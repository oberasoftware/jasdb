/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.btreeplus.caching;

import com.oberasoftware.jasdb.api.caching.CacheEntry;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.BtreePlusBlockPersister;

import java.util.concurrent.atomic.AtomicInteger;

public class IndexBlockEntry implements CacheEntry<IndexBlock> {
    private AtomicInteger token = new AtomicInteger(0);
    private long blockPointer;
	private IndexBlock indexBlock;
    private BtreePlusBlockPersister persister;

	public IndexBlockEntry(BtreePlusBlockPersister persister, IndexBlock indexBlock) {
        this.persister = persister;
		this.indexBlock = indexBlock;
        this.blockPointer = indexBlock.getPosition();
	}

    public void requestToken() {
        token.incrementAndGet();
    }

    public void releaseToken() {
        token.decrementAndGet();
    }

    @Override
    public boolean isInUse() {
        return token.get() > 0;
    }

    @Override
    public long memorySize() {
        return indexBlock.memorySize();
    }

    @Override
    public IndexBlock getValue() {
        return indexBlock;
    }

    @Override
	public boolean equals(Object object) {
        return object instanceof IndexBlock && object.hashCode() == hashCode();
	}

	@Override
	public int hashCode() {
		return Long.valueOf(blockPointer).hashCode();
	}

    @Override
    public void release() throws JasDBStorageException {
        persister.persistBlock(indexBlock);
    }

    @Override
    public String toString() {
        return "IndexBlockEntry{" +
                "blockPointer=" + blockPointer +
                ", token=" + token +
                '}';
    }
}
