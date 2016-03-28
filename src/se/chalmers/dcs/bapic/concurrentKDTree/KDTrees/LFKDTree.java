/*
 * To change this license header, choose License Headers in Project Properties. To change this
 * template file, choose Tools | Templates and open the template in the editor.
 */
package se.chalmers.dcs.bapic.concurrentKDTree.KDTrees;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import se.chalmers.dcs.bapic.concurrentKDTree.utils.*;

/**
 *
 * @author bapic
 */
public class LFKDTree<T> implements KDTreeADT<T> {

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

        public Neighbour(Node<T> currentNode, double currentDistance) {
            this.currentNode = currentNode;
            this.currentDistance = currentDistance;
        }
    }

    private static class LinearizedNeighbour<T> extends Neighbour<T> {

        LinearizedNeighbour(Neighbour<T> n) {
            super(n.currentNode, n.currentDistance);
        }
    }

    private static class NNCollector<T> {

        double[] target;
        volatile Neighbour<T> collectedNeighbour;
        volatile Neighbour<T> reportedNeighbour;
        volatile NNCollector<T> next;
        boolean isActive;

        public NNCollector(double[] target, Neighbour<T> n, NNCollector<T> nex) {
            this.target = target;
            this.collectedNeighbour = n;
            this.reportedNeighbour = n;
            this.next = nex;
            this.isActive = true;
        }

        public NNCollector() {
        }
    }

    private static class TerminalNNC<T> extends NNCollector<T> {

        public TerminalNNC(double[] target, Neighbour<T> n, NNCollector<T> nex) {
            super(target, n, nex);
        }
    }

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

        LeafNode(final double[] key, final T val) {
            super(key, 0);
            this.value = val;
        }

        LeafNode(final double[] key, final Node<T> r, final T val) {
            super(key, 0, null, r, null);
            this.value = val;
        }
    }

    protected static class MLeafNode<T> extends LeafNode<T> {

        MLeafNode(LeafNode<T> n) {
            super(n.key, n.value);
        }
    }

    final Node<T> root;
    final int k;
    private transient volatile NNCollector<T> nnPointer, terminalNN;
    private static final AtomicReferenceFieldUpdater<Node, Node> leftUpdater
                                                                 = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "left");
    private static final AtomicReferenceFieldUpdater<Node, Node> rightUpdater
                                                                 = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "right");
    private static final AtomicReferenceFieldUpdater<Node, Node> blUpdater
                                                                 = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "parent");
    private static final AtomicReferenceFieldUpdater<NNCollector, NNCollector> nextUpdater
                                                                               = AtomicReferenceFieldUpdater.newUpdater(NNCollector.class, NNCollector.class, "next");
    private static final AtomicReferenceFieldUpdater<NNCollector, Neighbour> reportUpdater
                                                                             = AtomicReferenceFieldUpdater.newUpdater(NNCollector.class, Neighbour.class,
                    "reportedNeighbour");
    private static final AtomicReferenceFieldUpdater<NNCollector, Neighbour> neighborUpdater
                                                                             = AtomicReferenceFieldUpdater.newUpdater(NNCollector.class, Neighbour.class,
                    "collectedNeighbour");

    public LFKDTree(int k) {
        this.k = k;
        double[] m_keyMax2 = new double[k];
        for (int i = 0; i < k; i ++) {
            m_keyMax2[i] = 0.9 * Double.MAX_VALUE;
        }
        int rootDim = 0;
        root = new Node<T>(new double[]{Double.MAX_VALUE}, rootDim, new LeafNode<T>(m_keyMax2, null),
                new LeafNode<T>(null, null), null);
        terminalNN = new TerminalNNC<>(m_keyMax2, null, null);
        nnPointer = new NNCollector<>(m_keyMax2, null, terminalNN);
    }

    private NNCollector<T> getNext(NNCollector<T> n) {
        NNCollector<T> next = n.next;
        return next.target == null ? next.next : next;
    }

    private Node<T> getRef(Node<T> n) {
        if (n.left == n) {
            return n.right;
        }
        return n;
    }

    private Node<T> getChild(final Node<T> n, boolean childDir) {
        return childDir ? getRef(n.left) : getRef(n.right);
    }

    private T process(NNCollector<T> n) {
        return ((LeafNode<T>) (n.reportedNeighbour.currentDistance < n.collectedNeighbour.currentDistance
                               ? n.reportedNeighbour.currentNode : n.collectedNeighbour.currentNode)).value;
    }

    //finds the next guess in the best first search
    private void findNext(double[] key, double[] VisitedHRect, Window<T> currentWin) {
        // VisitedHRect has (2*k+1) elements; the last one is the squared distance from the current
        // node.
        Node<T> parent = currentWin.parent, current = currentWin.current;
        double[] curKey = current.key;
        boolean turn = currentWin.turn;
        while (parent != root) {
            double x = (parent.key[0] - key[parent.dim]);
            if (Math.abs(x) < VisitedHRect[2 * k] && ((turn && parent.key[0] >= VisitedHRect[parent.dim])
                                                      || ( ! turn && parent.key[0] <= VisitedHRect[k + parent.dim]))) {
                // Here the checks are in the opposite directions
                turn =  ! turn;
                current = getChild(parent, turn);
                Window<T> cw = new Window<>(parent, current, turn);
                seek(cw, key, VisitedHRect);
                parent = cw.parent;
                current = cw.current;
                turn = cw.turn;
                curKey = current.key;
                double currentDist = Tools.squaredDistance(key, curKey);
                if (currentDist < VisitedHRect[2 * k] && current.getClass() == LeafNode.class) {
                    VisitedHRect[2 * k] = currentDist;
                    break;
                }
            }
            else {
                current = parent;
                parent = parent.parent;
                turn = curKey[parent.dim] < parent.key[0];
                if (turn) {
                    VisitedHRect[parent.dim]
                    = parent.key[0] > VisitedHRect[parent.dim] ? parent.key[0] : VisitedHRect[parent.dim];
                }
                else {
                    VisitedHRect[k + parent.dim] = parent.key[0] < VisitedHRect[k + parent.dim]
                                                   ? parent.key[0] : VisitedHRect[k + parent.dim];
                }
            }
        }
        currentWin.parent = parent;
        currentWin.current = current;
        currentWin.turn = turn;
    }

    private void seek(Window<T> cw, double[] key, double[] VisitedHRect) {
        while (cw.current.getClass() == Node.class) {
            cw.parent = cw.current;
            cw.turn = key[cw.parent.dim] < cw.parent.key[0];
            cw.current = getChild(cw.parent, cw.turn);
            if (cw.turn) {
                VisitedHRect[cw.parent.dim] = cw.parent.key[0];
            }
            else {
                VisitedHRect[k + cw.parent.dim] = cw.parent.key[0];
            }
        }
    }

    private void deactivate(NNCollector<T> n) {

        Neighbour<T> nCollected = n.collectedNeighbour;
        while (nCollected.getClass() == Neighbour.class) {// put linearization mark on the next pointer
            neighborUpdater.compareAndSet(n, nCollected, new LinearizedNeighbour<>(nCollected));
            nCollected = n.collectedNeighbour;
        }

        n.isActive = false;

        Neighbour<T> nReport = n.reportedNeighbour;
        while (nReport.getClass() == Neighbour.class) {// put linearization mark on the next pointer
            reportUpdater.compareAndSet(n, nReport, new LinearizedNeighbour<>(nReport));
            nReport = n.reportedNeighbour;
        }
    }

    private void collect(double[] key, NNCollector<T> nnc, Window<T> currentWin,
            double[] VisitedHRect) {
        Neighbour<T> collectedNeighbor;
        double currentDist;
        up:
        while (currentWin.parent != root) {
            findNext(key, VisitedHRect, currentWin);
            while (true) {
                collectedNeighbor = nnc.collectedNeighbour;
                currentDist = collectedNeighbor.getClass() == LinearizedNeighbour.class ? 0
                              : Math.min(nnc.reportedNeighbour.currentDistance, collectedNeighbor.currentDistance);
                if (nnc.isActive && currentDist > 0) {
                    /**
                     * If the collected neighbor has been linearized, the CAS
                     * will fail
                     */
                    if (VisitedHRect[2 * k] < currentDist) {
                        if (neighborUpdater.compareAndSet(nnc, collectedNeighbor,
                                new Neighbour<T>(currentWin.current, VisitedHRect[2 * k]))) {
                            break;
                        }
                    }
                    else {
                        VisitedHRect[2 * k] = currentDist;
                        break;
                    }
                }
                else {
                    break up;
                }
            }
        }
    }

    private T clean(NNCollector<T> preNNC, NNCollector<T> nnc) {
        NNCollector<T> nextNNC, markedNNC;
        while (true) {
            nextNNC = nnc.next;
            if (nextNNC.target == null) {
                markedNNC = nextNNC;
                break;
            }
            markedNNC = nextNNC.getClass() == NNCollector.class ? new NNCollector<T>(null, null, nextNNC)
                        : new TerminalNNC<T>(null, null, nextNNC);
            if (nextUpdater.compareAndSet(nnc, nextNNC, markedNNC)) {
                break;
            }
        }
        return nextUpdater.compareAndSet(preNNC, nnc, markedNNC.next) ? process(nnc) : null;
    }

    private T linearizableNearest(double[] key) throws KeySizeException {
        boolean turn = true;
        Node<T> parent = root, current = getChild(parent, turn);
        double[] VisitedHRect = new double[2 * k + 1];// The first k elements are for max of the
        // rectangle and the next k elements are for min
        // of the rectangle

        for (int i = 0; i < k; i ++) {
            VisitedHRect[i] = Double.POSITIVE_INFINITY;
            VisitedHRect[k + i] = Double.NEGATIVE_INFINITY;
        }
        Window<T> cw = new Window<>(parent, current, turn);
        seek(cw, key, VisitedHRect);
        VisitedHRect[2 * k] = current.getClass() == MLeafNode.class ? Double.MAX_VALUE
                              : Tools.squaredDistance(key, cw.current.key);

        T returnVal = ((LeafNode<T>) cw.current).value;

        if (VisitedHRect[2 * k] != 0) {
            NNCollector<T> preNNC, currentNNC, nnc = null;
            int mode = 1;// mode:- 1 = INIT, 2 = inserted/located the collector, 3 = completed the
            // collection
            retry:
            while (true) {
                preNNC = nnPointer;
                currentNNC = getNext(preNNC);
                while (currentNNC.getClass() == NNCollector.class) {// loop until we reach the terminal
                    // range
                    if (nnc == currentNNC && mode == 3) {
                        if (null != (returnVal = clean(preNNC, nnc))) {
                            return returnVal;
                        }
                        continue retry;
                    }
                    if (Tools.equals(key, currentNNC.target) && currentNNC.isActive) {// traverse the range
                        // collectors list as
                        // the poresent one is
                        // disjoint
                        nnc = currentNNC;
                        mode = 2;
                        break;
                    }
                    else {
                        preNNC = currentNNC;
                        currentNNC = getNext(preNNC);
                    }
                }
                if (mode == 3) {
                    return process(nnc);
                }

                if (mode == 1) {
                    NNCollector<T> nextNNC = preNNC.next;
                    if (nextNNC == currentNNC) {
                        nnc = new NNCollector<T>(key, new Neighbour<T>(cw.current, VisitedHRect[2 * k]),
                                terminalNN);
                        if (nextUpdater.compareAndSet(preNNC, currentNNC, nnc)) {
                            mode = 2;
                        }
                    }
                    else if (nextNNC.target == null && nextNNC.next == currentNNC) {
                        nextUpdater.compareAndSet(preNNC, nextNNC, currentNNC);
                    }
                }
                if (mode == 2) {
                    collect(key, nnc, cw, VisitedHRect);
                    deactivate(nnc);
                    mode = 3;
                    if (null != (returnVal = clean(preNNC, nnc))) {
                        return returnVal;
                    }
                }
            }
        }
        current = cw.current;
        parent = cw.parent;
        syncWithNNQuery(current, parent);
        return returnVal;
    }

    private T sequentiallyConsistentNearest(double[] key) throws KeySizeException {
        boolean turn = true;
        Node<T> parent = root, current = root.left;
        double[] VisitedHRect = new double[2 * k + 1];
        // The first k elements are for max of the rectangle and the next k elements are for min of the rectangle
        Window<T> currentWin = new Window<>(parent, current, turn);
        seek(currentWin, key, VisitedHRect);

        if (VisitedHRect[2 * k] != 0) {
            T returnVal = (T) current.key;
            while (currentWin.parent != root) {
                findNext(key, VisitedHRect, currentWin);
                returnVal = ((LeafNode<T>) currentWin.current).value;
            }
            return returnVal;
        }
        return (T) current.key;
    }

    @Override
    public T nearest(double[] key, boolean linearizable) throws KeySizeException {
        return linearizable ? linearizableNearest(key) : sequentiallyConsistentNearest(key);
    }

    private Neighbour<T> isNeighbour(Node<T> current, double[] target,
            Neighbour<T> collectedNeighbour, Neighbour<T> reportedNeighbour) {
        double distToTarget = Tools.squaredDistance(current.key, target);
        return distToTarget < collectedNeighbour.currentDistance
               && distToTarget < reportedNeighbour.currentDistance
               ? new Neighbour<T>(current, distToTarget) : null;
    }

    private void report(Node<T> current, NNCollector<T> nnc) {
        Neighbour<T> reportedNeighbour, updatedNeighbour;
        while (true) {
            reportedNeighbour = nnc.reportedNeighbour;
            updatedNeighbour = reportedNeighbour.getClass() == LinearizedNeighbour.class ? null
                               : isNeighbour(current, nnc.target, nnc.collectedNeighbour, reportedNeighbour);
            if (updatedNeighbour != null) {
                /**
                 * if reported neighbor has been linearized or a better neighbor
                 * has been reported then break
                 */
                if (reportUpdater.compareAndSet(nnc, reportedNeighbour, updatedNeighbour)) {
                    break;
                }
            }
            else {
                break;
            }
        }
    }

    private boolean checkValidNode(Node<T> current, Node<T> parent) {
        double[] key = current.key;
        Node<T> parentLink = getChild(parent, key[parent.dim] < parent.key[0]);
        while (parentLink.getClass() == Node.class) {// there may have been add or remove of nodes so
            // check the connection between parent and the node
            parent = parentLink;
            parentLink = getChild(parent, key[parent.dim] < parent.key[0]);
        }
        return (parentLink == current);
    }

    private void syncWithNNQuery(Node<T> current, Node<T> parent) {
        NNCollector<T> r = nnPointer.next, nextNN;
        while (r.getClass() == NNCollector.class) {
            if (r.isActive
                && isNeighbour(current, r.target, r.collectedNeighbour, r.reportedNeighbour) != null) {
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
                nextNN = r.next;
                r = nextNN.target != null ? nextNN : nextNN.next;
            }
        }
    }

    @Override
    public final boolean contains(final double[] key) {
        Node<T> parent = root;
        Node<T> current = getChild(parent, true);
        while (current.getClass() == Node.class) {// unless current becomes a clean or unclean leaf node
            parent = current;
            current = getChild(parent, key[parent.dim] < parent.key[0]);

        }
        if ( ! Tools.equals(k, key, current.key)) {
            return false;
        }
        if (current.getClass() == LeafNode.class) {
            syncWithNNQuery(current, parent);
            return true;
        }
        return false;
    }

    @Override
    public final boolean add(final double[] key, T value) {
        boolean turn = true;// left:- true, right:- false.
        Node<T> newInternal;
        Node<T> parent = root;
        Node<T> current = getChild(parent, turn);
        LeafNode<T> newLeafNode;
        double[] curKey;
        while (true) {
            while (current.getClass() == Node.class) {// unless current becomes a clean or unclean leaf
                // node
                parent = current;
                turn = key[parent.dim] < parent.key[0];
                current = getChild(parent, turn);
            }

            if (current.getClass() == LeafNode.class) {// if the current is a clean link, initialize the
                // new internal node and perform CAS to add the
                // node
                curKey = current.key;
                if (Tools.equals(k, key, curKey)) {
                    syncWithNNQuery(current, parent);
                    return false;
                }
                Node child = turn ? parent.left : parent.right;
                if (getRef(child) == current) {
                    if (child == current) {
                        int d, x;
                        d = x = 0;
                        double diff = 0, di;
                        do {
                            di = Math.abs(curKey[x] - key[x]);
                            if (di > diff) {
                                diff = di;
                                d = x;
                            }
                            x = (x + 1) % k;
                        }
                        while (x != 0);
                        while (key[d] == curKey[d]) {
                            d = (d + 1) % k;
                        }
                        newLeafNode = new LeafNode<T>(key, value);
                        newInternal = key[d] < curKey[d]
                                      ? new Node(new double[]{curKey[d] * 0.5 + key[d] * 0.5}, d, newLeafNode,
                                new LeafNode<T>(curKey, ((LeafNode<T>) current).value), parent)
                                      : new Node(new double[]{curKey[d] * 0.5 + key[d] * 0.5}, d,
                                new LeafNode<T>(curKey, ((LeafNode<T>) current).value), newLeafNode, parent);
                        if (turn
                            ? (parent.left == current ? leftUpdater.compareAndSet(parent, current, newInternal)
                               : false)
                            : (parent.right == current
                               ? rightUpdater.compareAndSet(parent, current, newInternal) : false)) {
                            syncWithNNQuery(newLeafNode, newInternal);
                            return true;
                        }
                    }
                    else {
                        Node<T> grParent = parent.parent;
                        parent
                        = helpFlagged(grParent, parent, current, current.key[grParent.dim] < grParent.key[0]);
                    }
                }
            }
            else {// if current is a marked link then helpMarked is performed to update the parent
                parent = helpMarked(parent, current, turn);
            }
            turn = key[parent.dim] < parent.key[0];
            current = getChild(parent, turn);
        }
    }

    @Override
    public final boolean remove(final double[] key) {
        boolean turn = true;// left:- true, right:- false.
        Node<T> parent = root;
        Node<T> current = getChild(parent, turn);
        while (true) {
            while (current.getClass() == Node.class) {// unless current becomes a clean or unclean leaf
                // node
                parent = current;
                turn = key[parent.dim] < parent.key[0];
                current = getChild(parent, turn);
            }

            if (current.getClass() == LeafNode.class) {// if the current is a clean link, mark it and move
                // ahead to complete the remove
                if ( ! Tools.equals(k, key, current.key)) {
                    return false;
                }
                Node child = turn ? parent.left : parent.right;
                if (getRef(child) == current) {
                    if (child == current) {
                        MLeafNode<T> markedLink = new MLeafNode<T>((LeafNode<T>) current);
                        if (turn
                            ? (parent.left == current ? leftUpdater.compareAndSet(parent, current, markedLink)
                               : false)
                            : (parent.right == current ? rightUpdater.compareAndSet(parent, current, markedLink)
                               : false)) {
                            helpMarked(parent, markedLink, turn);
                            return true;
                        }
                    }
                    else {
                        Node<T> grParent = parent.parent;
                        parent
                        = helpFlagged(grParent, parent, current, current.key[grParent.dim] < grParent.key[0]);
                    }
                }
            }
            else {// if current is a marked link then helpMarked is performed to update the parent
                return false;
            }
            turn = key[parent.dim] < parent.key[0];
            current = getChild(parent, turn);
        }
    }

    protected Node<T> helpMarked(Node<T> parent, Node<T> current, boolean childDir) {
        Node<T> grParent = parent.parent;
        Node<T> gpLink;
        boolean parentDir;
        while (true) {
            parentDir = current.key[grParent.dim] < grParent.key[0];
            gpLink = parentDir ? grParent.left : grParent.right;// parentDir ? grParent.left :
            // grParent.right;
            if (gpLink == parent) {
                Node<T> taggedGPLink = new Node<T>(null, 0, parent);
                taggedGPLink.left = taggedGPLink;
                taggedGPLink.parent = childDir ? taggedGPLink : parent;// depending on child we assign the
                // tagged link containing the
                // siblingDir;
                // if the parent link of taggedGPLink points to itself, it indicates a leftTaggedNode and if
                // it points to the right-child, it is a
                // rightTaggedLink
                if (parentDir
                    ? (grParent.left == gpLink ? leftUpdater.compareAndSet(grParent, gpLink, taggedGPLink)
                       : false)
                    : (grParent.right == gpLink ? rightUpdater.compareAndSet(grParent, gpLink, taggedGPLink)
                       : false)) {
                    helpTagged(grParent, taggedGPLink, parentDir);
                    break;
                }
            }
            // Now if gpLink is not poiting to the parent then there could be following possibilities
            // indicating the remove has not yet completed-
            // (a) gpLink has been tagged by a helping operation or a remove of the sibling (b) it has
            // been flagged by a remove of the sibling of the parent
            // or (c) it has been connected to the current with mark on by a remove of the sibling

            if (gpLink.left == gpLink && gpLink.right == parent) {// the gpLink is still connected to
                // parent and tagged or flagged
                if (gpLink.parent == null) {// gpLink is flagged by a remove of the sibling of parent;
                    // flagged-link has its parent null while left points to itself
                    Node<T> grGrParent = grParent.parent;
                    grParent = helpFlagged(grGrParent, grParent, parent,
                            current.key[grGrParent.dim] < grGrParent.key[0]);
                    continue;
                }
                else {// gpLink has been tagged by either a helping operation or a remove of the sibling
                    helpTagged(grParent, gpLink, parentDir);
                    if ((gpLink.parent == gpLink) == childDir) {// if it was a helping operation; which we
                        // check by checking the direction stored at
                        // the ta
                        // tagged link as the lefttaggedlink has its parent pointer pointing to itself
                        break;
                    }
                    else {
                        parent = grParent;
                    }
                }
            }
            else if (gpLink == current) {// gpLink is connected to the current after the sibling has
                // been removed
                parent = grParent;
            }
            else {
                break;
            }
            grParent = parent.parent;
            childDir = current.key[parent.dim] < parent.key[0];
        }
        return grParent;
    }

    public Node<T> appendFlag(Node<T> parent, boolean childDir) {
        while (true) {
            Node<T> child = childDir ? parent.left : parent.right;
            if (child.getClass() == MLeafNode.class) {
                return child;
            }
            if (child.left == child) {
                if (child.parent == null) {
                    return child.right;
                }
                else {
                    helpTagged(parent, child, childDir);
                }
            }
            else {
                Node<T> flaggedChild = new Node(null, 0, child);
                flaggedChild.left = flaggedChild;
                if ((childDir ? leftUpdater : rightUpdater).compareAndSet(parent, child, flaggedChild)) {
                    return child;
                }
            }
        }
    }

    protected void helpTagged(Node<T> grParent, Node<T> parentLink, boolean parentDir) {
        Node<T> parent = parentLink.right;
        boolean siblingDir = parentLink.parent != parentLink;

        Node<T> sibling = appendFlag(parent, siblingDir);
        if (sibling.parent == parent) {
            blUpdater.compareAndSet(sibling, parent, grParent);
        }
        (parentDir ? leftUpdater : rightUpdater).compareAndSet(grParent, parentLink, sibling);
    }

    protected Node<T> helpFlagged(Node<T> grParent, Node<T> parent, Node<T> sibling,
            boolean parentDir) {
        Node<T> parentLink = parentDir ? grParent.left : grParent.right;
        if (parentLink.left == parentLink && parentLink.right == parent) {
            if (sibling.parent == parent) {
                blUpdater.compareAndSet(sibling, parent, grParent);
            }
            (parentDir ? leftUpdater : rightUpdater).compareAndSet(grParent, parentLink, sibling);
        }
        return grParent;
    }
}

// @Override
// public T nearest(double[] key) throws KeySizeException {
// Node<T> parent = root, current = root.left;
// boolean turn = true;
// double[] nearestNeighbour, VisitedHRect = new double[2 * k];// The first k elements are for max
// of the rectangle and the next k elements are for min of the rectangle
// for (int i = 0; i < k; i++) {
// VisitedHRect[i] = Double.POSITIVE_INFINITY;
// VisitedHRect[k + i] = Double.NEGATIVE_INFINITY;
// }
// VisitedHRect[0] = parent.key[0];
// while (current.getClass() == Node.class) {
// parent = current.left == current ? current.right : current;
// turn = key[parent.dim] < parent.key[0];
// current = turn ? (parent.left == parent ? parent.right.left : parent.left)
// : (parent.left == parent ? parent.right.right : parent.right);
// if (turn) {
// VisitedHRect[parent.dim] = parent.key[0];
// } else {
// VisitedHRect[k + parent.dim] = parent.key[0];
// }
// }
// nearestNeighbour = current.key;
// double nearestDist = Tools.squaredDistance(key, nearestNeighbour), currentDist;
// if (nearestDist == 0) {
// return (T) nearestNeighbour;
// }
//
// while (parent != root) {
// double x = (parent.key[0] - key[parent.dim]);
// if (x * x < nearestDist && ((turn && parent.key[0] >= VisitedHRect[parent.dim]) || (!turn &&
// parent.key[0] <= VisitedHRect[k + parent.dim]))) {
// //Here the checks are in the opposite directions
// turn = !turn;
// current = turn ? (parent.left == parent ? parent.right.left : parent.left)
// : (parent.left == parent ? parent.right.right : parent.right);
// while (current.getClass() == Node.class) {
// parent = current.left == current ? current.right : current;
// turn = key[parent.dim] < parent.key[0];
// current = turn ? (parent.left == parent ? parent.right.left : parent.left)
// : (parent.left == parent ? parent.right.right : parent.right);
// if (turn) {
// VisitedHRect[parent.dim] = parent.key[0];
// } else {
// VisitedHRect[k + parent.dim] = parent.key[0];
// }
// }
// currentDist = Tools.squaredDistance(key, current.key);
// if (currentDist < nearestDist) {
// nearestNeighbour = current.key;
// nearestDist = currentDist;
// }
// } else {
// current = parent;
// parent = parent.parent;
// turn = current.key[parent.dim] < parent.key[0];
// if (turn) {
// VisitedHRect[parent.dim] = parent.key[0] > VisitedHRect[parent.dim] ? parent.key[0] :
// VisitedHRect[parent.dim];
// } else {
// VisitedHRect[k + parent.dim] = parent.key[0] < VisitedHRect[k + parent.dim] ? parent.key[0] :
// VisitedHRect[k + parent.dim];
// }
// }
// }
// return (T) nearestNeighbour;
// }
//
// @Override
// public final boolean add(final double[] key, T value) {
// Node<T> newInternal;
// Node<T> parent = root;//parent needs to be taken as RLink as may
// Node<T> current = root.left;
// boolean turn = true;//left:- true, right:- false.
// double[] curKey;
// while (true) {
// while (current.getClass() == Node.class) {//unless current becomes a clean or unclean leaf node
// parent = current.left == current ? current.right : current;
// turn = key[parent.dim] < parent.key[0];
// current = turn ? (parent.left == parent ? parent.right.left : parent.left)
// : (parent.left == parent ? parent.right.right : parent.right);
// }
//
// curKey = ((LeafNode<T>) current).key;
// if (Arrays.equals(key, curKey)) {
// return false;
// }
//
// if (current.getClass() == LeafNode.class) {//if the current is a clean link, initialize the new
// internal node and perform CAS to add the node
// int d = current.dim;
// while (key[d] == curKey[d]) {
// d = (d + 1) % k;
// }
// newInternal = key[d] < curKey[d]
// ? new Node(curKey, d, new LeafNode<T>(key, (d + 1) % k, value), new LeafNode<T>(curKey, (d + 1) %
// k, ((LeafNode<T>) current).value), parent)
// : new Node(key, d, new LeafNode<T>(curKey, (d + 1) % k, ((LeafNode<T>) current).value), new
// LeafNode<T>(key, (d + 1) % k, value), parent);
// if (turn ? (parent.left == current ? leftUpdater.compareAndSet(parent, current, newInternal) :
// false)
// : (parent.right == current ? rightUpdater.compareAndSet(parent, current, newInternal) : false)) {
// return true;
// }
// } else if (current.getClass() == MLeafNode.class) {//if current is a marked link then helpMarked
// is performed to update the parent
// parent = helpMarked(parent, current, turn);
// } else if (current.getClass() == FLeafNode.class) {//if current is a flagged or MarkedFlagged
// link perform helpflagged and then parent is updated because the older one turns out of BST
// parent = helpFlagged(parent, current.right);
// }
// turn = key[parent.dim] < parent.key[0];
// current = turn ? (parent.left == parent ? parent.right.left : parent.left)
// : (parent.left == parent ? parent.right.right : parent.right);
// }
// }
// @Override
// public final boolean contains(final double[] key) {
// Node<T> current = root;
// while (current.getClass() == Node.class) {
// current = key[current.dim] < current.key[current.dim]
// ? (current.left == current ? current.right.left : current.left)
// : (current.left == current ? current.right.right : current.right);
// }
// return (Arrays.equals(key, current.key));
// }
