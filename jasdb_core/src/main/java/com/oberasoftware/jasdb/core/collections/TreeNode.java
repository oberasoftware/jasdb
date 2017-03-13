package com.oberasoftware.jasdb.core.collections;

import java.util.List;
import java.util.NoSuchElementException;

/**
* @author Renze de Vries
*/
public class TreeNode<T extends Comparable<T>, V> {
    private static final int POSITIVE_IMBALANCE = 2;
    private static final int NEGATIVE_IMBALANCE = -POSITIVE_IMBALANCE;

    private T localKey;
    private V localValue;

    private TreeNode<T, V> left;
    private TreeNode<T, V> right;

    private int height = 0;

    TreeNode(T localKey, V localValue) {
        this.localKey = localKey;
        this.localValue = localValue;
    }

    public T getKey() {
        return localKey;
    }

    public V getValue() {
        return localValue;
    }

    public TreeNode<T, V> getLeft() {
        return left;
    }

    public TreeNode<T, V> getRight() {
        return right;
    }

    protected void reset() {
        if(left != null) {
            left.reset();
        }
        if(right != null) {
            right.reset();
        }

        left = null;
        right = null;
        localKey = null;
        localValue = null;
        height = 0;
    }

    public void range(List<V> values, T start, boolean inclusiveStart, T end, boolean inclusiveEnd) {
        int compareStart = start != null ? start.compareTo(localKey) : -1;
        int compareEnd = end != null ? end.compareTo(localKey) : -1;
        boolean evalStartInclude = start == null || (compareStart < 0 || (inclusiveStart && compareStart ==0));
        boolean evalEndInclude = end == null || (compareEnd > 0 || (inclusiveEnd && compareEnd == 0));

        if(left != null && evalStartInclude) {
            left.range(values, start, inclusiveStart, end, inclusiveEnd);
        }

        if(evalStartInclude && evalEndInclude) {
            values.add(localValue);
        }

        if(right != null && evalEndInclude) {
            right.range(values, start, inclusiveStart, end, inclusiveEnd);
        }
    }

    public void values(List<V> values) {
        if(left != null) {
            left.values(values);
        }
        values.add(localValue);
        if(right != null) {
            right.values(values);
        }
    }

    public void split(List<V> firstList, List<V> secondList, int maxSize) {
        if(left != null) {
            left.split(firstList, secondList, maxSize);
        }

        if(firstList.size() < maxSize) {
            firstList.add(localValue);
        } else {
            secondList.add(localValue);
        }

        if(right != null) {
            right.split(firstList, secondList, maxSize);
        }
    }

    protected void keys(List<T> keys) {
        if(left != null) {
            left.keys(keys);
        }
        keys.add(localKey);
        if(right != null) {
            right.keys(keys);
        }
    }

    public V get(T key) {
        int compare = key.compareTo(this.localKey);
        if(compare == 0) {
            return localValue;
        } else if(compare > 0 && right != null) {
            return right.get(key);
        } else if(compare < 0 && left != null) {
            return left.get(key);
        } else {
            return null;
        }
    }

    public V getPrevious(T key) {
        int compare = key.compareTo(this.localKey);
        if(compare == 0) {
            if(left != null) {
                return left.max().localValue;
            } else {
              return null;
            }
        } else if(compare > 0 && right != null) {
            V result = right.getPrevious(key);
            if(result == null) {
                return localValue;
            } else {
                return result;
            }
        } else if(compare < 0 && left != null) {
            return left.getPrevious(key);
        } else {
            throw new NoSuchElementException("No element found for key: " + key);
        }
    }

    public V getNext(T key) {
        int compare = key.compareTo(this.localKey);
        if(compare == 0) {
            if(right != null) {
                return right.min().localValue;
            } else {
                return null;
            }
        } else if(compare > 0 && right != null) {
            return right.getNext(key);
        } else if(compare < 0 && left != null) {
            V result = left.getNext(key);

            if(result == null) {
                return localValue;
            } else {
                return result;
            }
        } else {
            throw new NoSuchElementException("No element found for key: " + key);
        }
    }

    public void add(T key, V value) {
        int compare = key.compareTo(this.localKey);
        if(compare > 0) {
            if(right != null) {
                right.add(key, value);
            } else {
                right = new TreeNode<>(key, value);
            }
        } else {
            if(left != null) {
                left.add(key, value);
            } else {
                left = new TreeNode<>(key, value);
            }
        }

        recalculateHeight();
        balance();
    }

    public void remove(T key) {
        int compare = key.compareTo(this.localKey);
        if(compare == 0) {
            doRemove();
        } else if(compare > 0 && right != null) {
            right.remove(key);

            if(right.localKey == null) {
                right = null;
            }
        } else if(compare < 0 && left != null) {
            left.remove(key);

            if(left.localKey == null) {
                left = null;
            }
        }

        recalculateHeight();
        balance();
    }

    private void doRemove() {
        if(left == null && right == null) {
            //there are no children
            localKey = null;
        } else if(left != null && right == null) {
            //there is a left subtree, but no right subtree
            localKey = left.localKey;
            localValue = left.localValue;
            left = null;
        } else if(left == null) {
            //there is a right subtree, but no left subtree
            localKey = right.localKey;
            localValue = right.localValue;
            right = null;
        } else {
            //there is both a left and right subtree
            TreeNode<T, V> leftMax = left.max();
            T key = leftMax.localKey;
            V val = leftMax.localValue;

            left.remove(leftMax.localKey);
            if(left.localKey == null) {
                left = null;
            }

            this.localKey = key;
            this.localValue = val;

        }
    }

    public TreeNode<T, V> min() {
        if(left != null) {
            return left.min();
        } else {
            return this;
        }
    }

    public TreeNode<T, V> max() {
        if(right != null) {
            return right.max();
        } else {
            return this;
        }
    }

    private void recalculateHeight() {
        height = Math.max(left == null ? -1 : left.height, right == null ? -1 : right.height) + 1;
    }

    private int getBalanceFactor() {
        int leftHeight = left == null ? -1 : left.height;
        int rightHeight = right == null ? -1 : right.height;
        return rightHeight - leftHeight;
    }

    private void balance() {
        int diff = getBalanceFactor();
        if(diff == POSITIVE_IMBALANCE || diff == NEGATIVE_IMBALANCE) {
            if(diff == NEGATIVE_IMBALANCE) {
                if(left.getBalanceFactor() > 0) {
                    rotateLeft(left);
                }
                rotateRight(this);
            } else {
                if(right.getBalanceFactor() < 0) {
                    rotateRight(right);
                }
                rotateLeft(this);
            }
            recalculateHeight();
        }
    }

    private void rotateLeft(TreeNode<T, V> node) {
        TreeNode<T, V> oldRightNode = node.right;
        TreeNode<T, V> oldLeftNode = node.left;

        TreeNode<T, V> newLeft = new TreeNode<>(node.localKey, node.localValue);
        newLeft.left = oldLeftNode;
        newLeft.right = oldRightNode.left;

        node.localKey = oldRightNode.localKey;
        node.localValue = oldRightNode.localValue;
        node.right = oldRightNode.right;
        node.left = newLeft;

        if(node.right != null) {
            node.right.recalculateHeight();
        }
        if(node.left != null) {
            node.left.recalculateHeight();
        }
    }

    private void rotateRight(TreeNode<T, V> node) {
        TreeNode<T, V> oldRightNode = node.right;
        TreeNode<T, V> oldLeftNode = node.left;

        TreeNode<T, V> newRight = new TreeNode<>(node.localKey, node.localValue);
        newRight.right = oldRightNode;
        newRight.left = oldLeftNode.right;

        node.localKey = oldLeftNode.localKey;
        node.localValue = oldLeftNode.localValue;
        node.right = newRight;
        node.left = oldLeftNode.left;

        if(node.left != null) {
            node.left.recalculateHeight();
        }
        if(node.right != null) {
            node.right.recalculateHeight();
        }
    }

    public String toString(int depth) {
        StringBuilder builder = new StringBuilder();
        builder.append(localKey.toString());

        String indent = "";
        for(int i=0; i<depth; i++) {
            indent += "\t";
        }

        depth = depth + 1;
        if(left != null) {
            builder.append("\n").append(indent).append("left (height ").append(left.height);
            builder.append("): ").append(left.toString(depth));
        }
        if(right != null) {
            builder.append("\n").append(indent).append("right (height ").append(right.height);
            builder.append("): ").append(right.toString(depth));
        }

        return builder.toString();
    }
}
