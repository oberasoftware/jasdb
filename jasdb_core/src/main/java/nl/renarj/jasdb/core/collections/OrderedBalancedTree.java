/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class OrderedBalancedTree<T extends Comparable<T>, V> implements Iterable<V> {
    protected TreeNode<T, V> root;
    private int size;

    public OrderedBalancedTree() {
    }
    
    public void put(T item, V value) {
        if(root == null) {
            root = new TreeNode<>(item, value);
        } else {
            root.add(item, value);
        }
        size++;
    }
    
    public void remove(T key) {
        if(root != null) {
            root.remove(key);
            if(root.getKey() == null) {
                //root was deleted, and was last node
                root = null;
            }
            size--;
        }
    }
    
    public boolean contains(T key) {
        return get(key) != null;
    }

    public V get(T key) {
        if(root != null) {
            return root.get(key);
        }
        return null;
    }
    
    public V first() {
        if(root != null) {
            return root.min().getValue();
        }
        return null;
    }

    public V last() {
        if(root != null) {
            return root.max().getValue();
        }
        return null;
    }

    public V previous(T key) {
        if(root != null) {
            return root.getPrevious(key);
        }

        return null;
    }

    public V next(T key) {
        if(root != null) {
            return root.getNext(key);
        }
        return null;
    }

    public int size() {
        return this.size;
    }

    public List<V> range(T start, boolean includeStart, T end, boolean includeEnd) {
        if(root != null) {
            List<V> values = new ArrayList<>(size());
            root.range(values, start, includeStart, end, includeEnd);
            return values;
        }

        return Collections.emptyList();
    }
    
    public List<V> values() {
        List<V> values = new ArrayList<>(size());

        if(root != null) {
            root.values(values);
        }
        
        return values;
    }

    @SuppressWarnings("unchecked")
    public List<V>[] split() {
        int maxSize = size / 2;
        List<V> firstValues = new ArrayList<>(maxSize);
        List<V> secondValues = new ArrayList<>(maxSize);

        if(root != null) {
            root.split(firstValues, secondValues, maxSize);
        }

        return new List[]{firstValues, secondValues};
    }

    @Override
    public Iterator<V> iterator() {
        return values().iterator();
    }

    public List<T> keys() {
        List<T> keys = new ArrayList<>(size());
        
        if(root != null) {
            root.keys(keys);
        }
        
        return keys;
    }

    public void reset() {
        if(root != null) root.reset();
        root = null;
        size = 0;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();

        if(root != null) {
            builder.append("\n").append(root.toString(0));
        }
        
        return builder.toString();
    }
}
