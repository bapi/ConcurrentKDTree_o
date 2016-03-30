/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.chalmers.dcs.bapic.concurrentKDTree.KDTrees;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import se.chalmers.dcs.bapic.concurrentKDTree.utils.*;

/**
 *
 * @author bapic
 */
public class LBKDTree<T> implements KDTreeADT<T> {

    class Window<T> {

        Node<T> parent, current;
        boolean turn;

        public Window(Node<T> parent, Node<T> current, boolean turn) {
            this.parent = parent;
            this.current = current;
            this.turn = turn;
        }
    }

    private static class Neighbour<T> {

        Node<T> currentNode;
        double currentDistance;
        boolean isFinal;

        public Neighbour(Node<T> currentNode, double currentDistance) {
            this.currentNode = currentNode;
            this.currentDistance = currentDistance;
            this.isFinal = false;
        }
    }

    private static class NNCollector<T> {

        double[] target;
        Neighbour<T> collectedNeighbour;
        Neighbour<T> reportedNeighbour;
        volatile Lock collectLock, reportLock, listLock;
        NNCollector<T> next;
        boolean isActive, reportActive, isMarked;

        public NNCollector(double[] target, Neighbour<T> n, NNCollector<T> nex) {
            this.target = target;
            this.collectedNeighbour = n;
            this.reportedNeighbour = n;
            this.next = nex;
            this.collectLock = new ReentrantLock();
            this.reportLock = new ReentrantLock();
            this.listLock = new ReentrantLock();
            this.isActive = true;
            this.reportActive = true;
            this.isMarked = false;
        }

        public NNCollector() {
        }
    }

    protected static class Node<T> {

        final double[] key;
        volatile Node<T> left, right, parent;
        final int dim;
        volatile Lock lock;

        Node(final double[] key, final int d, final Node<T> l, final Node<T> r, final Node<T> p) {
            this.key = key;
            this.left = l;
            this.right = r;
            this.parent = p;
            this.dim = d;
            lock = new ReentrantLock();
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
        boolean isDeleted;

        LeafNode(final double[] key, final int dim, final T val) {
            super(key, dim);
            this.value = val;
            this.isDeleted = false;
        }
    }

    final Node<T> root;
    final int k;
    private transient volatile NNCollector<T> nnPointer, terminalNN;

    public LBKDTree(int k) {
        this.k = k;
        double[] m_keyMax1 = new double[k];
        double[] m_keyMax2 = new double[k];
        for (int i = 0; i < k; i ++) {
            m_keyMax1[i] = Double.MAX_VALUE;
            m_keyMax2[i] = 0.9 * Double.MAX_VALUE;
        }
        int rootDim = 0;
        root = new Node<T>(m_keyMax1, rootDim, new LeafNode<T>(m_keyMax2, (rootDim + 1) % k, null), new LeafNode<T>(m_keyMax1, (rootDim + 1) % k, null), null);
        terminalNN = new NNCollector<>(m_keyMax1, null, null);
        nnPointer = new NNCollector<>(m_keyMax2, null, terminalNN);
    }

    private T process(NNCollector<T> n) {
        return ((LeafNode<T>) (n.reportedNeighbour.currentDistance < n.collectedNeighbour.currentDistance
                               ? n.reportedNeighbour.currentNode : n.collectedNeighbour.currentNode)).value;
    }

    private void findNext(double[] key, double[] VisitedHRect, Window<T> currentWin) {//VisitedHRect has 2*k+1 elements; the last one is
        //the squared distance from the current node.
        Node<T> parent = currentWin.parent, current = currentWin.current;
        boolean turn = currentWin.turn;
        while (parent != root) {
            double x = (parent.key[parent.dim] - key[parent.dim]);
            if (x * x < VisitedHRect[2 * k] && ((turn && parent.key[parent.dim] >= VisitedHRect[parent.dim]) || ( ! turn && parent.key[parent.dim] <= VisitedHRect[k + parent.dim]))) {
                //Here the checks are in the opposite directions
                turn =  ! turn;
                current = turn ? (parent.left == parent ? parent.right.left : parent.left)
                          : (parent.left == parent ? parent.right.right : parent.right);
                while (current.getClass() == Node.class) {
                    parent = current.left == current ? current.right : current;
                    turn = key[parent.dim] < parent.key[parent.dim];
                    current = turn ? (parent.left == parent ? parent.right.left : parent.left)
                              : (parent.left == parent ? parent.right.right : parent.right);
                    if (turn) {
                        VisitedHRect[parent.dim] = parent.key[parent.dim];
                    }
                    else {
                        VisitedHRect[k + parent.dim] = parent.key[parent.dim];
                    }
                }
                double currentDist = Tools.squaredDistance(key, current.key);
                if (currentDist < VisitedHRect[2 * k] &&  ! ((LeafNode<T>) current).isDeleted) {
                    VisitedHRect[2 * k] = currentDist;
                    break;
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
        currentWin.parent = parent;
        currentWin.current = current;
        currentWin.turn = turn;
    }

    private Window<T> seek(double[] key, double[] VisitedHRect, Node<T> parent, Node<T> current, boolean turn) {//here VisitedHRect has 2*k elements
        for (int i = 0; i < k; i ++) {
            VisitedHRect[i] = Double.POSITIVE_INFINITY;
            VisitedHRect[k + i] = Double.NEGATIVE_INFINITY;
        }
        while (current.getClass() == Node.class) {
            parent = current.left == current ? current.right : current;
            turn = key[parent.dim] < parent.key[parent.dim];
            current = turn ? (parent.left == parent ? parent.right.left : parent.left)
                      : (parent.left == parent ? parent.right.right : parent.right);
            if (turn) {
                VisitedHRect[parent.dim] = parent.key[parent.dim];
            }
            else {
                VisitedHRect[k + parent.dim] = parent.key[parent.dim];
            }
        }
        VisitedHRect[2 * k] = Tools.squaredDistance(key, current.key);
        return new Window<T>(parent, current, turn);
    }

    private void deactivate(NNCollector<T> n) {
        if (n.isActive) {
            n.collectLock.lock();
            try {
                n.isActive = false;
            }
            catch (Exception e) {
            }
            finally {
                n.collectLock.unlock();
            }
        }
        if (n.reportActive) {
            n.reportLock.lock();
            try {
                n.reportActive = false;
            }
            catch (Exception e) {
            }
            finally {
                n.reportLock.unlock();
            }
        }
    }

    private T clean(NNCollector<T> preNNC, NNCollector<T> nnc) {
        T returnVal = null;
        preNNC.listLock.lock();
        try {
            nnc.listLock.lock();
            try {
                if (preNNC.next == nnc &&  ! preNNC.isMarked &&  ! nnc.isMarked) {
                    nnc.isMarked = true;
                    preNNC.next = nnc.next;
                    returnVal = process(nnc);
                }
            }
            catch (Exception e) {
            }
            finally {
                nnc.listLock.unlock();
            }
        }
        catch (Exception e) {
        }
        finally {
            preNNC.listLock.unlock();
        }
        return returnVal;
    }

    private T linearizableNearest(double[] key) throws KeySizeException {
        boolean turn = true;
        Node<T> parent = root, current = root.left;
        double[] VisitedHRect = new double[2 * k + 1];// The first k elements are for max of the rectangle and the next k elements are for min of the rectangle
        Window<T> currentWin = seek(key, VisitedHRect, parent, current, turn);
        T returnVal = ((LeafNode<T>) currentWin.current).value;

        if (VisitedHRect[2 * k] != 0) {
            NNCollector<T> preNNC, currentNNC, nnc = null;
            double currentDist;
            int mode = 1;//mode:- 1 = INIT, 2 = inserted/located the collector, 3 = completed the collection
            retry:
            while (true) {
                preNNC = nnPointer;
                currentNNC = preNNC.next;
                while (currentNNC != terminalNN) {//loop until we reach the terminal range
                    if (nnc == currentNNC && mode == 3) {
                        if (null != (returnVal = clean(preNNC, nnc))) {
                            return returnVal;
                        }
                        continue retry;
                    }
                    if (Tools.equals(key, currentNNC.target) && currentNNC.isActive) {
                        nnc = currentNNC;
                        mode = 2;
                        break;
                    }
                    else {
                        preNNC = currentNNC;
                        currentNNC = currentNNC.next;
                    }
                }

                if (mode == 3) {
                    return process(nnc);
                }

                if (mode == 1) {
                    preNNC.listLock.lock();
                    try {
                        currentNNC.listLock.lock();
                        try {
                            if (preNNC.next == currentNNC &&  ! preNNC.isMarked &&  ! currentNNC.isMarked) {
                                nnc = new NNCollector<T>(key, new Neighbour<T>(currentWin.current, VisitedHRect[2 * k]), terminalNN);
                                preNNC.next = nnc;
                                mode = 2;
                            }
                        }
                        catch (Exception e) {
                        }
                        finally {
                            currentNNC.listLock.unlock();
                        }
                    }
                    catch (Exception e) {
                    }
                    finally {
                        preNNC.listLock.unlock();
                    }
                }
                if (mode == 2) {
                    while (currentWin.parent != root) {
                        findNext(key, VisitedHRect, currentWin);
                        nnc.collectLock.lock();
                        try {
                            currentDist = Math.min(nnc.reportedNeighbour.currentDistance, nnc.collectedNeighbour.currentDistance);
                            if (nnc.isActive && currentDist > 0) {
                                if (VisitedHRect[2 * k] < currentDist) {
                                    nnc.collectedNeighbour = new Neighbour<T>(currentWin.current, VisitedHRect[2 * k]);
                                }
                                else {
                                    VisitedHRect[2 * k] = currentDist;
                                }
                            }
                            else {
                                break;
                            }
                        }
                        catch (Exception e) {
                        }
                        finally {
                            nnc.collectLock.unlock();
                        }
                    }
                    deactivate(nnc);
                    mode = 3;
                    if (null != (returnVal = clean(preNNC, nnc))) {
                        return returnVal;
                    }
                }
            }
        }
        current = currentWin.current;
        parent = currentWin.parent;
        syncWithNNQuery(current, parent);
        return returnVal;
    }

    private T sequentiallyConsistentNearest(double[] key) throws KeySizeException {
        boolean turn = true;
        Node<T> parent = root, current = root.left;
        double[] VisitedHRect = new double[2 * k + 1];// The first k elements are for max of the rectangle and the next k elements are for min of the rectangle

        Window<T> currentWin = seek(key, VisitedHRect, parent, current, turn);

        if (VisitedHRect[2 * k] != 0) {
            T returnVal = (T) current.key;
            while (currentWin.parent != root) {
                findNext(key, VisitedHRect, currentWin);

                returnVal = ((LeafNode<T>) currentWin.current).value;
            }
            return returnVal;
        }
        return (T) ((LeafNode<T>) current).value;
    }

    @Override
    public T nearest(double[] key, boolean linearizable) throws KeySizeException {
        return linearizable ? linearizableNearest(key) : sequentiallyConsistentNearest(key);
    }

    private Neighbour<T> isNeighbour(Node<T> current, double[] target, Neighbour<T> collectedNeighbour, Neighbour<T> reportedNeighbour) {
        double distToTarget = Tools.squaredDistance(current.key, target);
        return distToTarget < collectedNeighbour.currentDistance
               && distToTarget < reportedNeighbour.currentDistance
               ? new Neighbour<T>(current, distToTarget) : null;
    }

    private void report(Node<T> current, NNCollector<T> nnc) {
        nnc.reportLock.lock();
        try {
            if (nnc.reportActive) {
                Neighbour<T> updatedNeighbour = isNeighbour(current, nnc.target, nnc.collectedNeighbour, nnc.reportedNeighbour);
                if (updatedNeighbour != null) {
                    nnc.reportedNeighbour = updatedNeighbour;
                }
            }
        }
        catch (Exception e) {
        }
        finally {
            nnc.reportLock.unlock();
        }
    }

    private boolean checkValidNode(Node<T> current, Node<T> parent) {
        double[] key = current.key;
        Node<T> parentLink = key[parent.dim] < parent.key[parent.dim] ? parent.left : parent.right;
        if (parentLink != current) {
            while (parentLink.getClass() == Node.class) {//there may have been add or remove of nodes so check the connection between parent and the node
                parent = parentLink;
                parentLink = key[parent.dim] < parent.key[parent.dim] ? (parent.left == parent ? parent.right.left : parent.left)
                             : (parent.left == parent ? parent.right.right : parent.right);
            }
        }
        return (parentLink.equals(current));
    }

    private void syncWithNNQuery(Node<T> current, Node<T> parent) {
        NNCollector<T> r = nnPointer.next;
        while ( ! r.equals(terminalNN)) {
            if (r.isActive && isNeighbour(current, r.target, r.collectedNeighbour, r.reportedNeighbour) != null) {
                /**
                 * If after the node was added, it has been removed by a
                 * concurrent operation then it must not be reported
                 */
                if (checkValidNode(current, parent)) {
                    report(current, r);
                }
                else {
                    break;
                }
            }
            else {
                r = r.next;
            }
        }
    }

    @Override
    public final boolean contains(final double[] key) {
        Node<T> parent = root;//parent needs to be taken as RLink as may 
        Node<T> current = root.left;
        while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
            parent = current.left == current ? current.right : current;
            current = key[parent.dim] < parent.key[parent.dim] ? (parent.left) : (parent.right);;
        }
        if ( ! Tools.equals(k, key, current.key)) {
            return false;
        }
        if ( ! ((LeafNode<T>) current).isDeleted) {
            syncWithNNQuery(current, parent);
            return true;
        }
        return false;
    }

    @Override
    public final boolean add(final double[] key, T value) {
        Node<T> newInternal;
        boolean turn = true, parentDir;//left:- true, right:- false.boolean parentDir;
        double[] curKey;
        Node<T> parent = root;//parent needs to be taken as RLink as may 
        Node<T> current = root.left;
        Node<T> grParent;
        LeafNode<T> newLeafNode;

        while (true) {
            while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
                parent = current;
                turn = key[parent.dim] < parent.key[parent.dim];
                current = turn ? (parent.left) : (parent.right);
            }

            curKey = ((LeafNode<T>) current).key;
            if (Tools.equals(k, key, curKey)) {
                if ( ! ((LeafNode<T>) current).isDeleted) {
                    syncWithNNQuery(current, parent);
                    return false;
                }
            }
            else {
                parent.lock.lock();
                try {
                    grParent = parent.parent;
                    parentDir = grParent == null || parent.key[grParent.dim] < grParent.key[grParent.dim];
                    if ((grParent == null || (parentDir ? grParent.left : grParent.right) == parent) && (turn ? parent.left : parent.right) == current) {
//                        int d = current.dim;
//                        while (key[d] == curKey[d]) {
//                            d = (d + 1) % k;
//                        }
//                        newInternal = key[d] < curKey[d]
//                                ? new Node(curKey, d, new LeafNode<T>(key, (d + 1) % k, value), new LeafNode<T>(curKey, (d + 1) % k, ((LeafNode<T>) current).value), parent)
//                                : new Node(key, d, new LeafNode<T>(curKey, (d + 1) % k, ((LeafNode<T>) current).value), new LeafNode<T>(key, (d + 1) % k, value), parent);
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
                        return true;
                    }
                }
                catch (Exception e) {
                }
                finally {
                    parent.lock.unlock();
                }
            }
            grParent = parent.parent;
            if (grParent != null) {
                parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
                while (grParent != null && (parentDir ? grParent.left : grParent.right) != parent) {
                    parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
                    parent = grParent;
                    grParent = parent.parent;
                }
            }
            turn = key[parent.dim] < parent.key[parent.dim];
            current = turn ? (parent.left) : (parent.right);
        }
    }

    @Override
    public final boolean remove(final double[] key) {
        boolean turn = true;//left:- true, right:- false.
        Node<T> parent = root;//parent needs to be taken as RLink as may 
        Node<T> current = root.left;
        Node<T> grParent;
        boolean parentDir;
        while (true) {

            while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
                parent = current;
                turn = key[parent.dim] < parent.key[parent.dim];
                current = turn ? (parent.left) : (parent.right);
            }
            if ( ! Tools.equals(k, key, current.key)) {
                return false;
            }
            grParent = parent.parent;

            grParent.lock.lock();

            try {
                parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
                if (parent.parent == grParent && (parentDir ? grParent.left : grParent.right) == parent) {
                    parent.lock.lock();
                    try {
                        if (parent.parent == grParent && (turn ? parent.left : parent.right) == current) {
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
                            return true;
                        }
                    }
                    catch (Exception e) {
                    }
                    finally {
                        parent.lock.unlock();
                    }
                }
            }
            catch (Exception e) {
            }
            finally {
                grParent.lock.unlock();
            }
            grParent = parent.parent;
            parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
            while (grParent != null && (parentDir ? grParent.left : grParent.right) != parent) {
                parentDir = parent.key[grParent.dim] < grParent.key[grParent.dim];
                parent = grParent;
                grParent = parent.parent;
            }
            turn = key[parent.dim] < parent.key[parent.dim];
            current = turn ? (parent.left) : (parent.right);
        }
    }
}
