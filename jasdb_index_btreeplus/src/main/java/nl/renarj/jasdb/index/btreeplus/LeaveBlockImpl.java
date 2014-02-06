/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.collections.OrderedBalancedTree;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.utils.ReadWriteLock;
import nl.renarj.jasdb.index.btreeplus.locking.LockIntentType;
import nl.renarj.jasdb.index.btreeplus.persistence.BlockTypes;
import nl.renarj.jasdb.index.keys.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class LeaveBlockImpl implements LeaveBlock {
    private static final Logger log = LoggerFactory.getLogger(LeaveBlockImpl.class);

    private OrderedBalancedTree<Key, Key> leaves;
    private BlockPersister persister;

    private LeaveBlockProperties leaveProperties;

    private ReadWriteLock lockManager;

    private long memorySize;

    public LeaveBlockImpl(BlockPersister persister, DataBlock dataBlock, long parentBlock, boolean modified) {
        this.leaves = new OrderedBalancedTree<>();
        this.leaveProperties = new LeaveBlockProperties(dataBlock, -1, -1, parentBlock);
        this.leaveProperties.setModified(modified);
        this.lockManager = new ReadWriteLock();
        this.persister = persister;
    }

    @Override
    public Key getKey(Key key) {
        return leaves.get(key);
    }

    @Override
    public DataBlock getDataBlock() {
        return leaveProperties.getDataBlock();
    }

    @Override
    public void close() throws JasDBStorageException {
        leaves.reset();
        leaveProperties = null;
        persister = null;
        leaves = null;
    }

    @Override
    public List<Key> getKeyRange(Key start, boolean includeStart, Key end, boolean includeEnd) {
        return leaves.range(start, includeStart, end, includeEnd);
    }

    @Override
    public LeaveBlockImpl findLeaveBlock(LockIntentType intent, Key key) throws JasDBStorageException {
        return this;
    }

    @Override
    public LeaveBlock findFirstLeaveBlock(LockIntentType intentType) throws JasDBStorageException {
        return this;
    }

    public void insertKey(Key key) throws JasDBStorageException {
        leaves.put(key, key);
        memorySize += key.size();
        leaveProperties.setModified(true);

        handleBlockOverflow();
    }

    private void handleBlockOverflow() throws JasDBStorageException {
        if(leaves.size() > persister.getMaxKeys()) {
            List<Key>[] splittedKeys = leaves.split();
            this.leaves.reset();

            /* this block keeps representing the right half, the last current block key == max right */
            List<Key> rightKeys = splittedKeys[1];
            addKeys(rightKeys);

            List<Key> leftKeys = splittedKeys[0];
            long currentPrevious = leaveProperties.getPreviousBlock();
            LeaveBlockImpl leftLeaveBlock = (LeaveBlockImpl) persister.createBlock(BlockTypes.LEAVEBLOCK, leaveProperties.getParentPointer());
            leftLeaveBlock.setPrevious(currentPrevious);
            leftLeaveBlock.setNext(getPosition());
            leftLeaveBlock.addKeys(leftKeys);

            this.recalculateMemorySize();
            leftLeaveBlock.recalculateMemorySize();

            if(currentPrevious != -1) {
                //we need to relink, there is a previous block present
                LeaveBlockImpl previousBlock = (LeaveBlockImpl) persister.loadBlock(currentPrevious);
                previousBlock.setNext(leftLeaveBlock.getPosition());
            }
            leaveProperties.setPreviousBlock(leftLeaveBlock.getPosition());

            TreeBlock parentBlock = (TreeBlock) persister.loadBlock(leaveProperties.getParentPointer());
            parentBlock.insertBlock(leftLeaveBlock.getLast(), leftLeaveBlock, this);
        }
    }

    protected void recalculateMemorySize() {
        this.memorySize = 0;
        for(Key key : leaves.values()) {
            memorySize += key.size();
        }
    }

    @Override
    public void updateKey(Key key) throws JasDBStorageException {
        Key foundKey = leaves.get(key);
        foundKey.setKeys(key.getKeys());
        leaveProperties.setModified(true);
    }

    @Override
    public void removeKey(Key key) throws JasDBStorageException {
        leaves.remove(key);
        leaveProperties.setModified(true);
        memorySize -= key.size();

        handleBlockUnderflow();
    }


    private void handleBlockUnderflow() throws JasDBStorageException {
        if(leaves.size() < persister.getMinKeys()) {
            LeaveBlockImpl leftLeave = null;
            LeaveBlockImpl rightLeave = null;
            if(leaveProperties.getPreviousBlock() != -1) {
                leftLeave = (LeaveBlockImpl) persister.loadBlock(leaveProperties.getPreviousBlock());
            }
            if(leaveProperties.getNextBlock() != -1) {
                rightLeave = (LeaveBlockImpl) persister.loadBlock(leaveProperties.getNextBlock());
            }

            TreeBlock parentBlock = (TreeBlock) persister.loadBlock(leaveProperties.getParentPointer());
            if(leftLeave != null && leftLeave.getParentPointer() == getParentPointer() && leftLeave.size() > persister.getMinKeys()) {
                log.debug("Borrowing from left leave");
                //we can borrow from left
                Key borrowKey = leftLeave.getLast();
                leftLeave.removeKeyInternal(borrowKey);
                leaves.put(borrowKey, borrowKey);
                leftLeave.recalculateMemorySize();

                parentBlock.updateBlockPointer(leftLeave.getLast(), leftLeave.getPosition(), getPosition());
            } else if(rightLeave != null && rightLeave.getParentPointer() == getParentPointer() && rightLeave.size() > persister.getMinKeys()) {
                log.debug("Borrowing from right leave");
                //we can borrow from right
                Key borrowKey = rightLeave.getFirst();
                rightLeave.removeKeyInternal(borrowKey);
                leaves.put(borrowKey, borrowKey);
                rightLeave.recalculateMemorySize();

                parentBlock.updateBlockPointer(borrowKey, getPosition(), rightLeave.getPosition());
            } else {
                mergeLeaves(parentBlock, leftLeave, rightLeave);
            }
        }
    }

    private void mergeLeaves(TreeBlock parentBlock, LeaveBlockImpl leftLeave, LeaveBlockImpl rightLeave) throws JasDBStorageException {
        //lets remove the block and update the admin above
        Key removeKey;
        if(leftLeave != null && leftLeave.getParentPointer() == getParentPointer()) {
            removeKey = getFirst();
            log.debug("Doing merge into left leave with remove key: {}", removeKey);
            leftLeave.addKeys(leaves.values());
            leftLeave.recalculateMemorySize();
        } else if(rightLeave != null && rightLeave.getParentPointer() == getParentPointer()) {
            removeKey = getLast();
            log.debug("Doing merge into right leave with remove key: {}", removeKey);
            rightLeave.addKeys(leaves.values());
            rightLeave.recalculateMemorySize();
        } else {
            throw new JasDBStorageException("Invalid index state there should always be a sibbling leave block");
        }
        if(leftLeave != null) {
            leftLeave.setNext(rightLeave != null ? rightLeave.getPosition() : -1);
        }
        if(rightLeave != null) {
            rightLeave.setPrevious(leftLeave != null ? leftLeave.getPosition() : -1);
        }

        leaves.reset();
        persister.markDeleted(this);
        parentBlock.removeBlockPointer(removeKey, this);
    }

    protected void removeKeyInternal(Key key) throws JasDBStorageException {
        leaves.remove(key);
        leaveProperties.setModified(true);
    }

    public void addKey(Key key) {
        leaves.put(key, key);
    }

    private void addKeys(List<Key> keys) {
        for(Key key : keys) {
            leaves.put(key, key);
        }
    }

    @Override
    public List<Key> getValues() {
        return leaves.values();
    }

    @Override
    public LeaveBlockProperties getProperties() {
        return leaveProperties;
    }

    @Override
    public BlockTypes getType() {
        return BlockTypes.LEAVEBLOCK;
    }

    @Override
    public boolean isModified() {
        return leaveProperties.isModified();
    }

    @Override
    public long getParentPointer() {
        return leaveProperties.getParentPointer();
    }

    @Override
    public void setParentPointer(long block) {
        leaveProperties.setParentBlock(block);
        leaveProperties.setModified(true);
    }

    @Override
    public boolean contains(Key key) {
        return leaves.contains(key);
    }

    @Override
    public int size() {
        return leaves.size();
    }

    @Override
    public long memorySize() {
        return memorySize;
    }

    @Override
    public long getPosition() {
        return leaveProperties.getPosition();
    }

    @Override
    public Key getLast() {
        return leaves.last();
    }

    @Override
    public Key getFirst() {
        return leaves.first();
    }

    @Override
    public void reset() {
        leaves.reset();
    }

    protected void setNext(long next) {
        leaveProperties.setNextBlock(next);
    }

    protected void setPrevious(long previous) {
        leaveProperties.setPreviousBlock(previous);
    }

    @Override
    public ReadWriteLock getLockManager() {
        return lockManager;
    }

    @Override
    public String toString() {
        return "LeaveBlockImpl{" +
                "leaveProperties=" + leaveProperties +
                ",size=" + leaves.size() +
                '}';
    }
}
