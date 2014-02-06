/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.storage.datablocks.DataBlock;

/**
 * @author Renze de Vries
 * Date: 5/25/12
 * Time: 10:49 AM
 */
public class LeaveBlockProperties {
    private DataBlock dataBlock;

    private long parentBlock;
    private long nextBlock;
    private long previousBlock;
    private boolean modified;

    public LeaveBlockProperties(DataBlock dataBlock, long next, long previous, long parentBlock) {
        this.dataBlock = dataBlock;
        this.parentBlock = parentBlock;
        this.nextBlock = next;
        this.previousBlock = previous;
    }

    public long getPosition() {
        return dataBlock.getPosition();
    }

    public DataBlock getDataBlock() {
        return dataBlock;
    }

    public long getParentPointer() {
        return parentBlock;
    }

    public void setParentBlock(long parentBlock) {
        this.parentBlock = parentBlock;
    }

    public long getNextBlock() {
        return nextBlock;
    }

    public void setNextBlock(long nextBlock) {
        this.nextBlock = nextBlock;
    }

    public long getPreviousBlock() {
        return previousBlock;
    }

    public void setPreviousBlock(long previousBlock) {
        this.previousBlock = previousBlock;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    @Override
    public String toString() {
        return "LeaveBlockProperties{" +
                "dataBlock=" + dataBlock +
                ", parentBlock=" + parentBlock +
                '}';
    }
}
