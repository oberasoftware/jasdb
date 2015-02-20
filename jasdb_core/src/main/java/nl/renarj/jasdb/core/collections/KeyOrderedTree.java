package nl.renarj.jasdb.core.collections;

import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.Key;

/**
 * @author Renze de Vries
 */
public class KeyOrderedTree<V> extends OrderedBalancedTree<Key, V> {
    public V getBefore(Key key) {
        if(getRoot() != null) {
            return before(getRoot(), key);
        }
        return null;
    }

    private V before(TreeNode<Key, V> currentNode, Key key) {
        int compare = key.compare(currentNode.getKey(), CompareMethod.BEFORE).getCompare();
        if(compare == 0) {
            return currentNode.getValue();
        } else if(compare > 0) {
            if(currentNode.getRight() != null) {
                TreeNode<Key, V> right = currentNode.getRight();
                if(key.compare(right.min().getKey(), CompareMethod.BEFORE).getCompare() < 0) {
                    return currentNode.getValue();
                } else {
                    return before(right, key);
                }
            }
            return currentNode.getValue();
        } else if(compare < 0) {
            if(currentNode.getLeft() != null) {
                TreeNode<Key, V> left = currentNode.getLeft();
                TreeNode<Key, V> leftMax = left.max();
                if(key.compare(leftMax.getKey(), CompareMethod.BEFORE).getCompare() > 0) {
                    return leftMax.getValue();
                } else {
                    return before(left, key);
                }
            }
            return currentNode.getValue();
        } else {
            return null;
        }
    }


}
