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
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlockImpl;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlockProperties;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class LeaveBlockFactory implements BlockFactory<LeaveBlock> {
    private static final Logger LOG = LoggerFactory.getLogger(LeaveBlockFactory.class);

    private BlockPersister persister;
    private KeyInfo keyInfo;

    private static final int PREVIOUS_LEAVE_INDEX = 4;
    private static final int NEXT_LEAVE_INDEX = 12;
    private static final int PARENT_BLOCK_INDEX = 20;
    private static final int AMOUNT_KEY_INDEX = 28;

    public LeaveBlockFactory(BtreePlusBlockPersister persister) {
        this.persister = persister;
        this.keyInfo = persister.getKeyInfo();
    }

    @Override
    public LeaveBlock loadBlock(DataBlock dataBlock) throws JasDBStorageException {
        LOG.debug("Loading block: {}", dataBlock);
        LeaveBlockImpl leaveBlock = createBlock(-1, dataBlock);
        LeaveBlockProperties properties = leaveBlock.getProperties();
        properties.setModified(false);

        int amountOfKeys = dataBlock.getHeader().getInt(AMOUNT_KEY_INDEX);
        long nextBlock = dataBlock.getHeader().getLong(NEXT_LEAVE_INDEX);
        long previousBlock = dataBlock.getHeader().getLong(PREVIOUS_LEAVE_INDEX);
        long parent = dataBlock.getHeader().getLong(PARENT_BLOCK_INDEX);

        int offset = 0;
        DataBlock currentBlock = dataBlock;
        for(int i=0; i<amountOfKeys; i++) {
            KeyLoadResult loadedKeyResult = keyInfo.loadKey(offset, currentBlock);
            leaveBlock.addKey(loadedKeyResult.getLoadedKey());

            currentBlock = loadedKeyResult.getEndBlock();
            offset = loadedKeyResult.getNextOffset();
        }
        properties.setNextBlock(nextBlock);
        properties.setPreviousBlock(previousBlock);
        properties.setModified(false);
        properties.setParentBlock(parent);

        return leaveBlock;
    }

    @Override
    public void persistBlock(LeaveBlock block) throws JasDBStorageException {
        if(block instanceof LeaveBlockImpl && block.isModified()) {
            LOG.debug("Persisting block at position: {}", block.getPosition());
            LeaveBlockImpl leaveBlock = (LeaveBlockImpl) block;
            List<Key> writeKeys = leaveBlock.getValues();
            int nrKeys = writeKeys.size();

            DataBlock dataBlock = block.getDataBlock();
            dataBlock.reset();

            dataBlock.getHeader().putLong(PREVIOUS_LEAVE_INDEX, leaveBlock.getProperties().getPreviousBlock());
            dataBlock.getHeader().putLong(NEXT_LEAVE_INDEX, leaveBlock.getProperties().getNextBlock());
            dataBlock.getHeader().putLong(PARENT_BLOCK_INDEX, leaveBlock.getParentPointer());
            dataBlock.getHeader().putInt(AMOUNT_KEY_INDEX, nrKeys);

            LOG.debug("Writing amount of keys: {}", nrKeys);

            for(Key key : writeKeys) {
                dataBlock = keyInfo.writeKey(key, dataBlock);
            }
        } else if(block.isModified()) {
            throw new JasDBStorageException("Unable to store block, unexpected type");
        }
    }

    @Override
    public LeaveBlockImpl createBlock(long parentBlock, DataBlock dataBlock) throws JasDBStorageException {
        LeaveBlockImpl leaveBlock = new LeaveBlockImpl(persister, dataBlock, parentBlock, true);

        return leaveBlock;
    }
}
