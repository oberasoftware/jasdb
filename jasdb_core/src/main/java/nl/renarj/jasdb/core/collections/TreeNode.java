package nl.renarj.jasdb.core.collections;

import java.util.List;
import java.util.NoSuchElementException;

/**
* @author Renze de Vries
*/
public class TreeNode<T extends Comparable<T>, V> {
    private T key;
    private V value;

    private TreeNode<T, V> left;
    private TreeNode<T, V> right;

    private int height = 0;

    TreeNode(T key, V value) {
        this.key = key;
        this.value = value;
    }

    public T getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public TreeNode getLeft() {
        return left;
    }

    public TreeNode getRight() {
        return right;
    }

    protected void reset() {
        if(left != null) left.reset();
        if(right != null) right.reset();

        left = null;
        right = null;
        key = null;
        value = null;
        height = 0;
    }

    public void range(List<V> values, T start, boolean inclusiveStart, T end, boolean inclusiveEnd) {
        int compareStart = start != null ? start.compareTo(key) : -1;
        int compareEnd = end != null ? end.compareTo(key) : -1;
        boolean evalStartInclude = start == null || (compareStart < 0 || (inclusiveStart && compareStart ==0));
        boolean evalEndInclude = end == null || (compareEnd > 0 || (inclusiveEnd && compareEnd == 0));

        if(left != null && evalStartInclude) left.range(values, start, inclusiveStart, end, inclusiveEnd);

        if(evalStartInclude && evalEndInclude) {
            values.add(value);
        }

        if(right != null && evalEndInclude) right.range(values, start, inclusiveStart, end, inclusiveEnd);
    }

    public void values(List<V> values) {
        if(left != null) left.values(values);
        values.add(value);
        if(right != null) right.values(values);
    }

    public void split(List<V> firstList, List<V> secondList, int maxSize) {
        if(left != null) left.split(firstList, secondList, maxSize);

        if(firstList.size() < maxSize) {
            firstList.add(value);
        } else {
            secondList.add(value);
        }

        if(right != null) right.split(firstList, secondList, maxSize);
    }

    protected void keys(List<T> keys) {
        if(left != null) left.keys(keys);
        keys.add(key);
        if(right != null) right.keys(keys);
    }

    public V get(T key) {
        int compare = key.compareTo(this.key);
        if(compare == 0) {
            return value;
        } else if(compare > 0 && right != null) {
            return right.get(key);
        } else if(compare < 0 && left != null) {
            return left.get(key);
        } else {
            return null;
        }
    }

    public V getPrevious(T key) {
        int compare = key.compareTo(this.key);
        if(compare == 0) {
            if(left != null) {
                return left.max().value;
            } else {
              return null;
            }
        } else if(compare > 0 && right != null) {
            V result = right.getPrevious(key);
            if(result == null) {
                return value;
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
        int compare = key.compareTo(this.key);
        if(compare == 0) {
            if(right != null) {
                return right.min().value;
            } else {
                return null;
            }
        } else if(compare > 0 && right != null) {
            return right.getNext(key);
        } else if(compare < 0 && left != null) {
            V result = left.getNext(key);

            if(result == null) {
                return value;
            } else {
                return result;
            }
        } else {
            throw new NoSuchElementException("No element found for key: " + key);
        }
    }

    public void add(T key, V value) {
        int compare = key.compareTo(this.key);
        if(compare > 0) {
            if(right != null) {
                right.add(key, value);
            } else {
                right = new TreeNode(key, value);
            }
        } else {
            if(left != null) {
                left.add(key, value);
            } else {
                left = new TreeNode(key, value);
            }
        }

        recalculateHeight();
        balance();
    }

    public void remove(T key) {
        int compare = key.compareTo(this.key);
        if(compare == 0) {
            doRemove();
        } else if(compare > 0 && right != null) {
            right.remove(key);

            if(right.key == null) {
                right = null;
            }
        } else if(compare < 0 && left != null) {
            left.remove(key);

            if(left.key == null) {
                left = null;
            }
        }

        recalculateHeight();
        balance();
    }

    private void doRemove() {
        if(left == null && right == null) {
            //there are no children
            key = null;
        } else if(left != null && right == null) {
            //there is a left subtree, but no right subtree
            key = left.key;
            value = left.value;
            left = null;
        } else if(left == null) {
            //there is a right subtree, but no left subtree
            key = right.key;
            value = right.value;
            right = null;
        } else {
            //there is both a left and right subtree
            TreeNode<T, V> leftMax = left.max();
            T key = leftMax.key;
            V val = leftMax.value;

            left.remove(leftMax.key);
            if(left.key == null) {
                left = null;
            }

            this.key = key;
            this.value = val;

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
        if(diff == 2 || diff == -2) {
            if(diff == -2) {
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

    private void rotateLeft(TreeNode node) {
        TreeNode oldRightNode = node.right;
        TreeNode oldLeftNode = node.left;

        TreeNode newLeft = new TreeNode(node.key, node.value);
        newLeft.left = oldLeftNode;
        newLeft.right = oldRightNode.left;

        node.key = oldRightNode.key;
        node.value = oldRightNode.value;
        node.right = oldRightNode.right;
        node.left = newLeft;

        if(node.right != null) node.right.recalculateHeight();
        if(node.left != null) node.left.recalculateHeight();
    }

    private void rotateRight(TreeNode node) {
        TreeNode oldRightNode = node.right;
        TreeNode oldLeftNode = node.left;

        TreeNode newRight = new TreeNode(node.key, node.value);
        newRight.right = oldRightNode;
        newRight.left = oldLeftNode.right;

        node.key = oldLeftNode.key;
        node.value = oldLeftNode.value;
        node.right = newRight;
        node.left = oldLeftNode.left;

        if(node.left != null) node.left.recalculateHeight();
        if(node.right != null) node.right.recalculateHeight();
    }

    public String toString(int depth) {
        StringBuilder builder = new StringBuilder();
        builder.append(key.toString());

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
