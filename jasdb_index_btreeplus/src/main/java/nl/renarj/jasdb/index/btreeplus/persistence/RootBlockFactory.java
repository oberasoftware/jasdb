/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus.persistence;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.btreeplus.RootBlock;
import nl.renarj.jasdb.index.btreeplus.TreeBlock;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;

/**
 * @author Renze de Vries
 */
public class RootBlockFactory extends NodeBlockFactory {

    private static final int ISLEAVE_INDEX = 16;

    private KeyInfo keyInfo;

    public RootBlockFactory(BtreePlusBlockPersister persister) {
        super(persister);
        this.keyInfo = persister.getKeyInfo();
    }

    public static void writeHeader(DataBlock block, boolean isLeave) throws JasDBStorageException {
        int isLeaveInt = isLeave ? 1 : -1;
        block.getHeader().putInt(ISLEAVE_INDEX, isLeaveInt);
        block.flush();
    }

    @Override
    public RootBlock loadBlock(DataBlock dataBlock) throws JasDBStorageException {
        RootBlock rootBlock = (RootBlock) super.loadBlock(dataBlock);

        rootBlock.setLeave(isRootLeave(rootBlock));
        rootBlock.setParentPointer(-1);

        return rootBlock;
    }

    @Override
    public void persistBlock(TreeBlock treeBlock) throws JasDBStorageException {
        writeHeader(treeBlock.getDataBlock(), ((RootBlock)treeBlock).isLeave());

        super.persistBlock(treeBlock);
    }

    @Override
    protected DataBlock writeKey(TreeBlock treeBlock, Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(isRootLeave((RootBlock)treeBlock)) {
            return keyInfo.writeKey(key, dataBlock);
        } else {
            return super.writeKey(treeBlock, key, dataBlock);
        }
    }

    @Override
    protected KeyLoadResult loadKeyResult(TreeBlock treeBlock, int offset, DataBlock dataBlock) throws JasDBStorageException {
        if(isRootLeave((RootBlock)treeBlock)) {
            return keyInfo.loadKey(offset, dataBlock);
        } else {
            return super.loadKeyResult(treeBlock, offset, dataBlock);
        }
    }

    private boolean isRootLeave(RootBlock rootBlock) throws JasDBStorageException {
        int isLeave = rootBlock.getDataBlock().getHeader().getInt(ISLEAVE_INDEX);

        return isLeave > 0;
    }


    @Override
    public RootBlock createBlock(long parentBlock, DataBlock dataBlock) throws JasDBStorageException {
        RootBlock rootBlock = new RootBlock(persister, dataBlock, true);
        if(dataBlock.getHeader().marker() == 0) {
            writeHeader(dataBlock, true);
        }


        return rootBlock;
    }
}
