/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.chalmers.dcs.bapic.concurrentKDTree.KDTrees;

import edu.wlu.cs.levy.CG.KDTree;
import edu.wlu.cs.levy.CG.KeyDuplicateException;
import edu.wlu.cs.levy.CG.KeyMissingException;
import edu.wlu.cs.levy.CG.KeySizeException;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.chalmers.dcs.bapic.concurrentKDTree.utils.*;

/**
 *
 * @author Ivan Walulya
 * @param <K>
 * @param <V>
 */
public class KDTreeLevyWrapper<V> implements KDTreeADT<V> {

    KDTree<V> kd;
    int DIM;

    public KDTreeLevyWrapper(int dim) {
        kd = new KDTree<>(dim);
        this.DIM = dim;
    }

    @Override
    public boolean contains(double[] key) throws DimensionLimitException {

        try {
            return (kd.search(key) != null);
        }
        catch (KeySizeException ex) {
            Logger.getLogger(KDTreeLevyWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    @Override
    public boolean add(double[] key, V value) throws DimensionLimitException {

        try {
            kd.insert(key, value);
        }
        catch (KeySizeException | KeyDuplicateException ex) {
            return false;//Logger.getLogger(KDTreeLevyWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    @Override
    public boolean remove(double[] key) throws DimensionLimitException {

        try {
            kd.delete(key);
            return true;

        }
        catch (KeySizeException ex) {
            Logger.getLogger(KDTreeLevyWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (KeyMissingException ex) {
            return false;//Logger.getLogger(KDTreeLevyWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }

        return false;
    }

    @Override
    public V nearest(double[] key, boolean linearizable) {
        try {
            return kd.nearest(key);
        }
        catch (KeySizeException | IllegalArgumentException ex) {
            Logger.getLogger(KDTreeLevyWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
