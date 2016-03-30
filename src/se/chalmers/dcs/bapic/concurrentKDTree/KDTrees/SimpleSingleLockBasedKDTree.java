/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.chalmers.dcs.bapic.concurrentKDTree.KDTrees;

//import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import se.chalmers.dcs.bapic.concurrentKDTree.utils.*;

/**
 *
 * @author bapic
 */
public class SimpleSingleLockBasedKDTree<T> implements KDTreeADT<T> {

    protected static class Node<T> {

        final double[] key;
        volatile Node<T> left, right, parent;
        final int dim;

        Node(final double[] key, final int d, final Node<T> l, final Node<T> r, final Node<T> p) {
            this.key = key;
            this.left = l;
            this.right = r;
            this.parent = p;
            this.dim = d;
        }

        Node(final double[] key, final int d) {
            this(key, d, null, null, null);
        }

        Node(final double[] key, final int d, final Node<T> r) {
            this(key, d, null, r, null);
        }
    }

    protected static class LeafNode<T> extends Node<T> {

        final T value;

        LeafNode(final double[] key, final int dim, final T val) {
            super(key, dim);
            this.value = val;
        }
    }

    final Node<T> root;
    private volatile ReentrantLock lock = new ReentrantLock();
    final int k;

    public SimpleSingleLockBasedKDTree(int k) {
        this.k = k;
        double[] m_keyMax1 = new double[k];
        double[] m_keyMax2 = new double[k];
        for (int i = 0; i < k; i ++) {
            m_keyMax1[i] = Double.MAX_VALUE;
            m_keyMax2[i] = 0.90 * Double.MAX_VALUE;
        }
        int rootDim = 0;
        root = new Node<T>(m_keyMax1, rootDim, new LeafNode<T>(m_keyMax2, (rootDim + 1) % k, null), new LeafNode<T>(m_keyMax1, (rootDim + 1) % k, null), null);
    }

    @Override
    public final boolean contains(final double[] key) {
        Node<T> current = root;
        while (current.getClass() == Node.class) {
            current = key[current.dim] < current.key[current.dim] ? (current.left) : (current.right);
        }
        boolean result = Tools.equals(k, key, current.key);
        return result;
    }

    @Override
    public final boolean add(final double[] key, T value) {
        Node<T> newInternal;
        boolean turn = true;//left:- true, right:- false.
        double[] curKey;
        boolean returnVlaue = false;
        LeafNode<T> newLeafNode;
        lock.lock();
        try {
            Node<T> parent = root;//parent needs to be taken as RLink as may 
            Node<T> current = root.left;

            while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
                parent = current;
                turn = key[parent.dim] < parent.key[parent.dim];
                current = turn ? (parent.left) : (parent.right);
            }

            curKey = ((LeafNode<T>) current).key;
            if (Tools.equals(k, key, curKey)) {
                return false;
            }
            int d, x;
            d = x = current.dim;
            double diff = 0, di;
            do {
                di = Math.abs(curKey[x] - key[x]);
                if (di > diff) {
                    diff = di;
                    d = x;
                }
                x = (x + 1) % k;
            }
            while (x != current.dim);
            while (key[d] == curKey[d]) {
                d = (d + 1) % k;
            }
            newLeafNode = new LeafNode<T>(key, (d + 1) % k, value);
            newInternal = key[d] < curKey[d]
                          ? new Node(curKey.clone(), d, newLeafNode, new LeafNode<T>(curKey, (d + 1) % k, ((LeafNode<T>) current).value), parent)
                          : new Node(key.clone(), d, new LeafNode<T>(curKey, (d + 1) % k, ((LeafNode<T>) current).value), newLeafNode, parent);
            newInternal.key[d] = curKey[d] * 0.5 + key[d] * 0.5;
            if (turn) {
                parent.left = newInternal;
            }
            else {
                parent.right = newInternal;
            }
            returnVlaue = true;
        }
        catch (Exception e) {
        }
        finally {
            lock.unlock();
        }
        return returnVlaue;
    }

    @Override
    public final boolean remove(final double[] key) {
        boolean returnVlaue = false;
        boolean turn = true;//left:- true, right:- false.
        lock.lock();
        try {
            Node<T> parent = root;//parent needs to be taken as RLink as may 
            Node<T> current = root.left;

            while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
                parent = current;
                turn = key[parent.dim] < parent.key[parent.dim];
                current = turn ? (parent.left) : (parent.right);
            }
            if ( ! Tools.equals(k, key, current.key)) {
                return false;
            }

            Node<T> grParent = parent.parent;
            boolean parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
            Node<T> sibling = (turn) ? parent.right : parent.left;
            if (sibling.getClass() == Node.class) {
                sibling.parent = grParent;
            }
            if (parentDir) {
                grParent.left = sibling;
            }
            else {
                grParent.right = sibling;
            }

            returnVlaue = true;

        }
        catch (Exception e) {
        }
        finally {
            lock.unlock();

        }
        return returnVlaue;
    }

    @Override
    public T nearest(double[] key, boolean linearizable) throws KeySizeException {
        T returnVal;
        Node<T> parent = root, current = root.left;
        boolean turn = true;
        double[] nearestNeighbour, VisitedHRect = new double[2 * k];// The first k elements are for max of the rectangle and the next k elements are for min of the rectangle
        for (int i = 0; i < k; i ++) {
            VisitedHRect[i] = Double.POSITIVE_INFINITY;
            VisitedHRect[k + i] = Double.NEGATIVE_INFINITY;
        }
        VisitedHRect[0] = parent.key[parent.dim];
        while (current.getClass() == Node.class) {
            parent = current;
            turn = key[parent.dim] < parent.key[parent.dim];
            current = turn ? (parent.left) : (parent.right);
            if (turn) {
                VisitedHRect[parent.dim] = parent.key[parent.dim];
            }
            else {
                VisitedHRect[k + parent.dim] = parent.key[parent.dim];
            }
        }
        nearestNeighbour = current.key;
        double nearestDist = Tools.squaredDistance(key, nearestNeighbour), currentDist;
        returnVal = ((LeafNode<T>) current).value;
        if (nearestDist != 0) {
            while (parent != root) {
                double x = (parent.key[parent.dim] - key[parent.dim]);
                if (x * x < nearestDist && ((turn && parent.key[parent.dim] >= VisitedHRect[parent.dim]) || ( ! turn && parent.key[parent.dim] <= VisitedHRect[k + parent.dim]))) {
                    //Here the checks are in the opposite directions
                    turn =  ! turn;
                    current = turn ? (parent.left == parent ? parent.right.left : parent.left)
                              : (parent.left == parent ? parent.right.right : parent.right);
                    while (current.getClass() == Node.class) {
                        parent = current;
                        turn = key[parent.dim] < parent.key[parent.dim];
                        current = turn ? (parent.left) : (parent.right);
                        if (turn) {
                            VisitedHRect[parent.dim] = parent.key[parent.dim];
                        }
                        else {
                            VisitedHRect[k + parent.dim] = parent.key[parent.dim];
                        }
                    }
                    currentDist = Tools.squaredDistance(key, current.key);
                    if (currentDist < nearestDist) {
                        nearestNeighbour = current.key;
                        nearestDist = currentDist;
                        returnVal = ((LeafNode<T>) current).value;
                    }
                }
                else {
                    current = parent;
                    parent = parent.parent;
                    turn = current.key[parent.dim] < parent.key[parent.dim];
                    if (turn) {
                        VisitedHRect[parent.dim] = parent.key[parent.dim] > VisitedHRect[parent.dim] ? parent.key[parent.dim] : VisitedHRect[parent.dim];
                    }
                    else {
                        VisitedHRect[k + parent.dim] = parent.key[parent.dim] < VisitedHRect[k + parent.dim] ? parent.key[parent.dim] : VisitedHRect[k + parent.dim];
                    }
                }
            }
        }
        return returnVal;
    }
}
