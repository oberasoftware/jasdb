/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.btreeplus.locking.LockIntentType;
import nl.renarj.jasdb.index.btreeplus.persistence.BlockTypes;
import nl.renarj.jasdb.index.btreeplus.persistence.RootBlockFactory;
import nl.renarj.jasdb.index.keys.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class RootBlock extends TreeBlock implements LeaveBlock {
    private static final Logger log = LoggerFactory.getLogger(RootBlock.class);

    private boolean isLeave = true;

    public RootBlock(BlockPersister persister, DataBlock dataBlock, boolean isLeave) {
        super(persister, dataBlock, -1);
        this.isLeave = isLeave;
    }

    @Override
    public long getParentPointer() {
        return getPosition();
    }

    @Override
    public void setParentPointer(long parentBlock) {
        super.setParentPointer(parentBlock);
    }

    @Override
    public BlockTypes getType() {
        return BlockTypes.ROOTBLOCK;
    }

    @Override
    public boolean contains(Key key) {
        return treeNodes.contains(key);
    }

    @Override
    public int size() {
        return treeNodes.size();
    }

    @Override
    public Key getKey(Key key) {
        return treeNodes.get(key).getKey();
    }

    @Override
    public LeaveBlockProperties getProperties() {
        return new LeaveBlockProperties(getDataBlock(), -1, -1, -1);
    }

    @Override
    public LeaveBlock findLeaveBlock(LockIntentType intent, Key key) throws JasDBStorageException {
        if(isLeave) {
            return this;
        } else {
            return super.findLeaveBlock(intent, key);
        }
    }

    @Override
    public LeaveBlock findFirstLeaveBlock(LockIntentType intentType) throws JasDBStorageException {
        if(isLeave) {
            return this;
        } else {
            return super.findFirstLeaveBlock(intentType);
        }
    }

    @Override
    public void insertKey(Key key) throws JasDBStorageException {
        if(isLeave) {
            addKey(key);
            modified = true;

            if(treeNodes.size() > persister.getMaxKeys()) {
                List<TreeNode>[] leaveValues = treeNodes.split();
                List<TreeNode> leftLeaves = leaveValues[0];
                List<TreeNode> rightLeaves = leaveValues[1];
                treeNodes.reset();

                LeaveBlockImpl leftLeaveBlock = (LeaveBlockImpl) persister.createBlock(BlockTypes.LEAVEBLOCK, getPosition());
                LeaveBlockImpl rightLeaveBlock = (LeaveBlockImpl) persister.createBlock(BlockTypes.LEAVEBLOCK, getPosition());
                leftLeaveBlock.setNext(rightLeaveBlock.getPosition());
                rightLeaveBlock.setPrevious(leftLeaveBlock.getPosition());

                addKeys(leftLeaves, leftLeaveBlock);
                addKeys(rightLeaves, rightLeaveBlock);

                Key promoteKey = leftLeaves.get(leftLeaves.size() - 1).getKey();
                TreeNode rootNode = new TreeNode(promoteKey, leftLeaveBlock.getPosition(), rightLeaveBlock.getPosition());
                treeNodes.put(promoteKey, rootNode);

                isLeave = false;
                RootBlockFactory.writeHeader(getDataBlock(), false);
            }
        } else {
            throw new JasDBStorageException("Unable to store key, root is not a leave");
        }
    }

    @Override
    protected void removeBlockPointer(Key minBlockValue, IndexBlock removedBlock) throws JasDBStorageException {
        modified = true;
        if(size() > 1) {
            TreeNode removeNode = treeNodes.getBefore(minBlockValue);
            log.debug("Removing from root: {}", removeNode);
            TreeNode next = treeNodes.next(removeNode.getKey());

            if(next != null && removeNode.getLeft() != removedBlock.getPosition()) {
                //we need to relink the next block
                next.setLeft(removeNode.getLeft());
            }
            treeNodes.remove(removeNode.getKey());
        } else {
            log.debug("Merging into root");
            TreeNode lastTreeNode = treeNodes.first();
            IndexBlock leftBlock = persister.loadBlock(lastTreeNode.getLeft());
            IndexBlock rightBlock = persister.loadBlock(lastTreeNode.getRight());

            IndexBlock targetBlock;
            if(leftBlock.getPosition() != removedBlock.getPosition()) {
                targetBlock = leftBlock;
            } else if(rightBlock.getPosition() != removedBlock.getPosition()) {
                targetBlock = rightBlock;
            } else {
                throw new JasDBStorageException("Unable to move up to root, invalid index structure");
            }

            treeNodes.reset();
            if(targetBlock instanceof LeaveBlock) {
                LeaveBlockImpl leaveBlock = (LeaveBlockImpl) targetBlock;
                isLeave = true;
                RootBlockFactory.writeHeader(getDataBlock(), true);
                for(Key key : leaveBlock.getValues()) {
                    addKey(key);
                }
            } else if(targetBlock instanceof TreeBlock) {
                TreeBlock treeBlock = (TreeBlock) targetBlock;
                addNodes(treeBlock.getNodes().values(), null, getPosition());
            }
        }
    }

    @Override
    public void updateKey(Key key) throws JasDBStorageException {
        if(isLeave) {
            Key foundKey = treeNodes.get(key).getKey();
            foundKey.setKeys(key.getKeys());
            modified = true;
        } else {
            throw new JasDBStorageException("Unable to update key, root is not a leave");
        }
    }

    @Override
    public void removeKey(Key key) throws JasDBStorageException {
        if(isLeave) {
            treeNodes.remove(key);
            modified = true;
        } else {
            throw new JasDBStorageException("Unable to remove key, root is not a leave");
        }
    }

    @Override
    public List<Key> getKeyRange(Key start, boolean includeStart, Key end, boolean includeEnd) {
        List<TreeNode> foundNodes = treeNodes.range(start, includeStart, end, includeEnd);
        List<Key> foundKeys = new ArrayList<Key>(foundNodes.size());
        for(TreeNode node : foundNodes) {
            foundKeys.add(node.getKey());
        }
        return foundKeys;
    }

    @Override
    public List<Key> getValues() {
        List<Key> rootKeys = new ArrayList<Key>(treeNodes.size());
        for(TreeNode treeNode : treeNodes.values()) {
            rootKeys.add(treeNode.getKey());
        }
        return rootKeys;
    }

    private void addKey(Key key) {
        treeNodes.put(key, new TreeNode(key, -1, -1));
    }

    protected void addKeys(List<TreeNode> nodes, LeaveBlockImpl targetLeave) {
        for(TreeNode node : nodes) {
            targetLeave.addKey(node.getKey());
        }
    }

    public void setLeave(boolean leave) {
        isLeave = leave;
    }

    public boolean isLeave() {
        return isLeave;
    }
}
