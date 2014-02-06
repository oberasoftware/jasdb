/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.index.keys.Key;

/**
 * @author Renze de Vries
 * Date: 5/23/12
 * Time: 11:14 PM
 */
public class TreeNode {
    private Key key;
    private long left;
    private long right;

    public TreeNode(Key key, long left, long right) {
        this.key = key;
        this.left = left;
        this.right = right;
    }

    public long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        this.left = left;
    }

    public long getRight() {
        return right;
    }

    public void setRight(long right) {
        this.right = right;
    }

    public Key getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
