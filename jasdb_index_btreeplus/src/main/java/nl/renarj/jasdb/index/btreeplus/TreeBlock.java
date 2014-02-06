/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.collections.KeyOrderedTree;
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
public class TreeBlock implements IndexBlock {
    private static final Logger log = LoggerFactory.getLogger(TreeBlock.class);

    protected final KeyOrderedTree<TreeNode> treeNodes;
    protected final BlockPersister persister;
    protected boolean modified;

    private DataBlock dataBlock;
    private long parentPointer;
    private ReadWriteLock lockManager;

    public TreeBlock(BlockPersister persister, DataBlock dataBlock, long parentPointer) {
        this(persister, dataBlock, parentPointer, false);
    }

    public TreeBlock(BlockPersister persister, DataBlock dataBlock, long parentPointer, boolean modified) {
        treeNodes = new KeyOrderedTree<>();
        this.lockManager = new ReadWriteLock();
        this.persister = persister;
        this.dataBlock = dataBlock;
        this.parentPointer = parentPointer;
        this.modified = modified;
    }

    @Override
    public DataBlock getDataBlock() {
        return dataBlock;
    }

    @Override
    public void close() throws JasDBStorageException {
        dataBlock = null;
        treeNodes.reset();
    }

    @Override
    public LeaveBlock findLeaveBlock(LockIntentType intent, Key key) throws JasDBStorageException {
        TreeNode closestNode = treeNodes.getBefore(key);
        Key nodeKey = closestNode.getKey();
        int compare = key.compareTo(nodeKey);

        long blockPointer;
        if(compare <= 0) {
            blockPointer = closestNode.getLeft();
        } else {
            blockPointer = closestNode.getRight();
        }
        IndexBlock block = persister.loadBlock(blockPointer);
        persister.getLockManager().acquireLock(intent, block);
        return block.findLeaveBlock(intent, key);
    }

    @Override
    public LeaveBlock findFirstLeaveBlock(LockIntentType intentType) throws JasDBStorageException {
        TreeNode firstNode = treeNodes.first();

        IndexBlock block = persister.loadBlock(firstNode.getLeft());
        persister.getLockManager().acquireLock(intentType, block);
        return block.findFirstLeaveBlock(intentType);
    }

    protected void insertBlock(Key key, IndexBlock leftChildBlock, IndexBlock rightChildBlock) throws JasDBStorageException {
        TreeNode node = new TreeNode(key, leftChildBlock.getPosition(), rightChildBlock.getPosition());
        treeNodes.put(key, node);
        modified = true;

        /* let's relink all the blocks */
        TreeNode previousNode = treeNodes.previous(key);
        TreeNode nextNode = treeNodes.next(key);
        if(previousNode != null && nextNode != null) {
            //we are adding in the middle
            previousNode.setRight(leftChildBlock.getPosition());
            nextNode.setLeft(rightChildBlock.getPosition());
        } else if(previousNode != null) {
            //we are adding in the end
            previousNode.setRight(leftChildBlock.getPosition());
        } else {
            //we must be adding to the beginning
            nextNode.setLeft(rightChildBlock.getPosition());
        }

        handleBlockOverflow();
    }

    private void handleBlockOverflow() throws JasDBStorageException {
        if(treeNodes.size() > persister.getMaxKeys()) {
            List<TreeNode>[] blockNodeSplit = treeNodes.split();
            List<TreeNode> leftBlockNodes = blockNodeSplit[0];
            List<TreeNode> rightBlockNodes = blockNodeSplit[1];
            treeNodes.reset();

            Key promoteKey = rightBlockNodes.get(0).getKey();
            TreeBlock leftBlock = (TreeBlock) persister.createBlock(BlockTypes.NODEBLOCK, getParentPointer());
            leftBlock.addNodes(leftBlockNodes, null, leftBlock.getPosition());

            if(parentPointer != -1) {
                //add the nodes, we use -1 as parent does not need to be changed
                addNodes(rightBlockNodes, promoteKey, getPosition());
                TreeBlock parentBlock = (TreeBlock) persister.loadBlock(parentPointer);
                parentBlock.insertBlock(promoteKey, leftBlock, this);
            } else {
                //we are at the root
                TreeBlock rightBlock = (TreeBlock) persister.createBlock(BlockTypes.NODEBLOCK, getPosition());
                rightBlock.addNodes(rightBlockNodes, promoteKey, rightBlock.getPosition());

                TreeNode node = new TreeNode(promoteKey, leftBlock.getPosition(), rightBlock.getPosition());
                treeNodes.put(node.getKey(), node);
            }
        }
    }

    protected void addNodes(List<TreeNode> nodes, Key exclude, long parentBlock) throws JasDBStorageException {
        for(TreeNode node : nodes) {
            boolean isExcluded = exclude != null && exclude.equals(node.getKey());
            if(parentBlock != -1 && (!isExcluded)) {
                if(node.getLeft() != -1) {
                    IndexBlock block = persister.loadBlock(node.getLeft());
                    block.setParentPointer(parentBlock);
                }

                if(node.getRight() != -1) {
                    IndexBlock block = persister.loadBlock(node.getRight());
                    block.setParentPointer(parentBlock);
                }
            }
            if(!isExcluded) {
                treeNodes.put(node.getKey(), node);
            }
        }
    }

    protected void updateBlockPointer(Key replaceKey, long leftBlock, long rightBlock) throws JasDBStorageException {
        modified = true;
        if(treeNodes.contains(replaceKey)) {
            treeNodes.remove(replaceKey);
            treeNodes.put(replaceKey, new TreeNode(replaceKey, leftBlock, rightBlock));
        } else {
            treeNodes.put(replaceKey, new TreeNode(replaceKey, leftBlock, rightBlock));

            TreeNode previousNode = treeNodes.previous(replaceKey);
            TreeNode nextNode = treeNodes.next(replaceKey);
            if(previousNode != null && nextNode != null) {
                //we are updating in the middle
                if(previousNode.getLeft() == leftBlock && previousNode.getRight() == rightBlock) {
                    //the previous node is the one we are replacing, lets remove it
                    treeNodes.remove(previousNode.getKey());
                } else if(nextNode.getLeft() == leftBlock && nextNode.getRight() == rightBlock) {
                    //the next node is the one we are replacing, lets remove it
                    treeNodes.remove(nextNode.getKey());
                } else {
                    throw new JasDBStorageException("Unable to update the pointer after remove, invalid state");
                }
            } else if(previousNode != null) {
                //we are updating at the end of the chain
                treeNodes.remove(previousNode.getKey());
            } else {
                //we must be updating in the beginning of the chain
                treeNodes.remove(nextNode.getKey());
            }
        }
    }

    protected void removeBlockPointer(Key minBlockValue, IndexBlock removedBlock) throws JasDBStorageException {
        TreeNode removeNode = treeNodes.getBefore(minBlockValue);
//        log.info("Closest remove node: {} nodes: {}", removeNode, treeNodes.size());
        TreeNode next = treeNodes.next(removeNode.getKey());

        if(next != null && removeNode.getLeft() != removedBlock.getPosition()) {
            //we need to relink the next block
            next.setLeft(removeNode.getLeft());
        }
        treeNodes.remove(removeNode.getKey());
        modified = true;

        handleBlockUnderflow();
    }

    protected void handleBlockUnderflow() throws JasDBStorageException {
        if(treeNodes.size() < persister.getMinKeys()) {
            log.debug("Handling block underflow");
            TreeBlock parentBlock = (TreeBlock) persister.loadBlock(parentPointer);
            long leftSibblingPointer = parentBlock.getLeftSibbling(this);
            long rightSibblingPointer = parentBlock.getRightSibbling(this);
            TreeBlock leftSibbling = null;
            TreeBlock rightSibbling = null;
            if(leftSibblingPointer != -1) {
                leftSibbling = (TreeBlock) persister.loadBlock(leftSibblingPointer);
            }
            if(rightSibblingPointer != -1) {
                rightSibbling = (TreeBlock) persister.loadBlock(rightSibblingPointer);
            }

            if(leftSibbling != null && leftSibbling.size() > persister.getMinKeys()) {
                handleBorrowLeft(parentBlock, leftSibbling);
            } else if(rightSibbling != null && rightSibbling.size() > persister.getMinKeys()) {
                handleBorrowRight(parentBlock, rightSibbling);
            } else {
                //nothing to borrow we need to merge
                handleMerge(parentBlock, leftSibbling, rightSibbling);
            }
        }
    }

    private void handleBorrowLeft(TreeBlock parentBlock, TreeBlock leftSibbling) throws JasDBStorageException {
        //we can borrow from left
        TreeNode node = leftSibbling.getNodes().last();

        //this block needs relinking
        IndexBlock danglingBlock = persister.loadBlock(node.getRight());
        danglingBlock.setParentPointer(getPosition());
        Key newKey = leftSibbling.getMax();

        leftSibbling.removeNodeInternal(node);
        TreeNode newNode = new TreeNode(newKey, danglingBlock.getPosition(), treeNodes.first().getLeft());
        treeNodes.put(newKey, newNode);

        parentBlock.updateBlockPointer(leftSibbling.getMax(), leftSibbling.getPosition(), getPosition());
    }

    private void handleBorrowRight(TreeBlock parentBlock, TreeBlock rightSibbling) throws JasDBStorageException {
        //we can borrow from right
        TreeNode rightFirstNode = rightSibbling.getNodes().first();

        IndexBlock danglingBlock = persister.loadBlock(rightFirstNode.getLeft());
        danglingBlock.setParentPointer(getPosition());
        Key newKey = getMax();

        rightSibbling.removeNodeInternal(rightFirstNode);
        TreeNode newNode = new TreeNode(newKey, treeNodes.last().getRight(), danglingBlock.getPosition());
        treeNodes.put(newKey, newNode);

        Key parentKey = getMax();
        parentBlock.updateBlockPointer(parentKey, getPosition(), rightSibbling.getPosition());
    }

    private void handleMerge(TreeBlock parentBlock, TreeBlock leftSibbling, TreeBlock rightSibbling) throws JasDBStorageException {
        Key removeKey;
        if(leftSibbling != null) {
            Key mergeKey = leftSibbling.getMax();
            TreeNode leftSibblingLastNode = leftSibbling.getNodes().last();
            TreeNode currentFirstNode = treeNodes.first();
            removeKey = currentFirstNode.getKey();

            TreeNode mergeNode = new TreeNode(mergeKey, leftSibblingLastNode.getRight(), currentFirstNode.getLeft());
            leftSibbling.addKey(mergeNode);
            leftSibbling.addNodes(treeNodes.values(), null, leftSibbling.getPosition());
        } else if(rightSibbling != null) {
            Key mergeKey = getMax();
            TreeNode rightFirstNode = rightSibbling.getNodes().first();
            TreeNode currentLastNode = treeNodes.last();
            removeKey = currentLastNode.getKey();

            TreeNode mergeNode = new TreeNode(mergeKey, currentLastNode.getRight(), rightFirstNode.getLeft());
            rightSibbling.addKey(mergeNode);
            rightSibbling.addNodes(treeNodes.values(), null, rightSibbling.getPosition());
        } else {
            throw new JasDBStorageException("Invalid index state there should always be a sibbling tree block");
        }

        treeNodes.reset();
        persister.markDeleted(this);
        parentBlock.removeBlockPointer(removeKey, this);
    }

    protected long getLeftSibbling(TreeBlock block) {
        TreeNode beforeNode = treeNodes.getBefore(block.getFirst());
        if(beforeNode.getLeft() != block.getPosition()) {
            return beforeNode.getLeft();
        } else {
            return -1;
        }
    }

    protected long getRightSibbling(TreeBlock block) {
        TreeNode beforeNode = treeNodes.getBefore(block.getLast());
        if(beforeNode.getRight() == block.getPosition()) {
            TreeNode nextNode = treeNodes.next(beforeNode.getKey());
            return nextNode != null ? nextNode.getRight() : -1;
        } else {
            //this only happens with one node
            return beforeNode.getRight();
        }
    }

    public void addKey(TreeNode node) {
        this.treeNodes.put(node.getKey(), node);
    }

    private void removeNodeInternal(TreeNode node) {
        this.treeNodes.remove(node.getKey());
    }

    public OrderedBalancedTree<Key, TreeNode> getNodes() {
        return treeNodes;
    }

    @Override
    public int size() {
        return treeNodes.size();
    }

    @Override
    public long memorySize() {
        return size() * persister.getKeyInfo().getKeySize();
    }

    @Override
    public BlockTypes getType() {
        return BlockTypes.NODEBLOCK;
    }

    @Override
    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    @Override
    public long getParentPointer() {
        return parentPointer;
    }

    @Override
    public void setParentPointer(long parentBlock) {
        this.parentPointer = parentBlock;
        this.modified = true;
    }

    @Override
    public long getPosition() {
        return dataBlock.getPosition();
    }

    @Override
    public Key getFirst() {
        return treeNodes.first().getKey();
    }

    @Override
    public Key getLast() {
        return treeNodes.last().getKey();
    }

    @Override
    public void reset() {
        treeNodes.reset();
    }

    @Override
    public ReadWriteLock getLockManager() {
        return lockManager;
    }

    protected Key getMin() throws JasDBStorageException {
        TreeNode node = treeNodes.first();
        IndexBlock leftBlock = persister.loadBlock(node.getLeft());
        if(leftBlock instanceof TreeBlock) {
            return ((TreeBlock)leftBlock).getMin();
        } else {
            return leftBlock.getFirst();
        }
    }

    protected Key getMax() throws JasDBStorageException {
        TreeNode node = treeNodes.last();
        IndexBlock rightBlock = persister.loadBlock(node.getRight());
        if(rightBlock instanceof TreeBlock) {
            return ((TreeBlock) rightBlock).getMax();
        } else {
            return rightBlock.getLast();
        }
    }
}
