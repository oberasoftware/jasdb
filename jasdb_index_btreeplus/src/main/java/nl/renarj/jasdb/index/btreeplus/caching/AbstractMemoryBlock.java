/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus.caching;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 * Date: 4/6/12
 * Time: 8:23 PM
 */
public abstract class AbstractMemoryBlock implements Comparable<AbstractMemoryBlock> {
    private long idleEntryId;

    public AbstractMemoryBlock() {

    }

    public long getIdleEntryId() {
        return idleEntryId;
    }

    public void setIdleEntryId(long idleEntryId) {
        this.idleEntryId = idleEntryId;
    }

    @Override
    public int compareTo(AbstractMemoryBlock arg0) {
        return Long.valueOf(idleEntryId).compareTo((arg0).idleEntryId);
    }

    public abstract void close() throws JasDBStorageException;
}
