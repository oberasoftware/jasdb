/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.btreeplus.persistence;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockResult;
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.TreeBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.TreeNode;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class NodeBlockFactory implements BlockFactory<TreeBlock> {
    private static final Logger LOG = LoggerFactory.getLogger(NodeBlockFactory.class);

    private static final int PARENTPOINTER_INDEX = 4;
    private static final int NODEAMOUNT_INDEX = 12;

    protected BlockPersister persister;

    private KeyInfo keyInfo;

    public NodeBlockFactory(BtreePlusBlockPersister persister) {
        this.keyInfo = persister.getKeyInfo();
        this.persister = persister;
    }

    @Override
    public TreeBlock loadBlock(DataBlock dataBlock) throws JasDBStorageException {
        TreeBlock treeBlock = createBlock(-1, dataBlock);
        treeBlock.setModified(false);
        treeBlock.setParentPointer(dataBlock.getHeader().getLong(PARENTPOINTER_INDEX));

        int nrNodes = dataBlock.getHeader().getInt(NODEAMOUNT_INDEX);

        int offset = 0;
        DataBlock currentBlock = dataBlock;
        for(int i=0; i<nrNodes; i++) {
            KeyLoadResult loadedKeyResult = loadKeyResult(treeBlock, offset, currentBlock);
            DataBlockResult<Long> leftPointerResult = loadedKeyResult.getEndBlock().loadLong(loadedKeyResult.getNextOffset());
            DataBlockResult<Long> rightPointerResult = leftPointerResult.getEndBlock().loadLong(leftPointerResult.getNextOffset());

            TreeNode node = new TreeNode(loadedKeyResult.getLoadedKey(), leftPointerResult.getValue(), rightPointerResult.getValue());
            treeBlock.addKey(node);

            LOG.trace("Loaded Key: {} with left: {} and right: {} on block: {}",
                    node.getKey(), node.getLeft(), node.getRight(), treeBlock.getPosition());

            offset = rightPointerResult.getNextOffset();
            currentBlock = rightPointerResult.getEndBlock();
        }

        return treeBlock;
    }

    protected KeyLoadResult loadKeyResult(TreeBlock treeBlock, int offset, DataBlock dataBlock) throws JasDBStorageException {
        return keyInfo.getKeyFactory().loadKey(offset, dataBlock);
    }

    @Override
    public void persistBlock(TreeBlock treeBlock) throws JasDBStorageException {
        if(treeBlock.isModified()) {
            LOG.debug("Persisting block at position: {}", treeBlock.getPosition());
            List<TreeNode> treeNodes = treeBlock.getNodes().values();

            DataBlock dataBlock = treeBlock.getDataBlock();
            dataBlock.reset();
            dataBlock.getHeader().putLong(PARENTPOINTER_INDEX, treeBlock.getParentPointer());
            dataBlock.getHeader().putInt(NODEAMOUNT_INDEX, treeNodes.size());

            for(TreeNode treeNode : treeNodes) {
                dataBlock = writeKey(treeBlock, treeNode.getKey(), dataBlock);
                dataBlock = dataBlock.writeLong(treeNode.getLeft()).getDataBlock();
                dataBlock = dataBlock.writeLong(treeNode.getRight()).getDataBlock();
            }
        }
    }

    protected DataBlock writeKey(TreeBlock treeBlock, Key key, DataBlock dataBlock) throws JasDBStorageException {
        return keyInfo.getKeyFactory().writeKey(key, dataBlock);
    }

    @Override
    public TreeBlock createBlock(long parentBlock, DataBlock dataBlock) throws JasDBStorageException {
        return new TreeBlock(persister, dataBlock, parentBlock, true);
    }
}
