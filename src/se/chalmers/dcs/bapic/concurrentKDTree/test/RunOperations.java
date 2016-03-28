/*
 * Copyright (c) 2015-2016, Bapi Chatterjee All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the
 * distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package se.chalmers.dcs.bapic.concurrentKDTree.test;

import java.io.IOException;
import org.apache.commons.math3.distribution.*;
import java.util.Random;
import se.chalmers.dcs.bapic.concurrentKDTree.utils.*;

/**
 *
 * @author bapic
 */
public class RunOperations implements Runnable {

    boolean testSanity;
    boolean linearizable;
    int threadId;
    int addPercent;
    int removePercent;
    int keyRange;
    int numberOfOps;
    int dimension;
    KDTreeADT set;
    Random randOp;
    Random randKey;
    AbstractIntegerDistribution z;
    int[] results, numberOfAdd, numberOfRemove;
    int[][] sanityAdds, sanityRemoves;

    /**
     *
     * @param s
     * @param tId
     * @param aP
     * @param rP
     * @param kR
     * @param results
     * @param sanityAdds
     * @param sanityRemoves
     * @param testSanity
     * @throws IOException
     */
    public RunOperations(KDTreeADT s, int tId, int aP, int rP, int kR, int dim, int[] results,
            int[][] sanityAdds, int[][] sanityRemoves, boolean testSanity, boolean linearizable)
            throws IOException {
        this.testSanity = testSanity;
        this.linearizable = linearizable;
        this.threadId = tId;
        this.addPercent = aP;
        this.removePercent = rP;
        this.keyRange = kR;
        this.dimension = dim;
        this.set = s;
        this.randOp = new Random(threadId);
        this.randKey = new Random(threadId);
        this.z = new ZipfDistribution(keyRange, 5.0);
        this.numberOfOps = 0;
        this.numberOfAdd = new int[kR];
        this.numberOfRemove = new int[kR];
        this.results = results;
        this.sanityAdds = sanityAdds;
        this.sanityRemoves = sanityRemoves;
    }

    private void benchMarkRun() {
        while ( ! RunController.startFlag);

        while ( ! RunController.stopFlag) {
            int chooseOperation = randOp.nextInt(100);
            double[] key = new double[dimension];
            for (int i = 0; i < dimension; i ++) {
                key[i] = Tools.randomInRange(randKey, 0, keyRange);
                // System.err.println(key[i]);
            }
            try {
                if (chooseOperation < addPercent) {
                    set.add(key, key);
                }
                else if (chooseOperation < removePercent) {
                    set.remove(key);
                }
                else {
                    set.nearest(key, linearizable);
                }
            }
            catch (DimensionLimitException e) {

            }
            numberOfOps ++;
        }

        results[threadId] = numberOfOps;
    }

    private void sanityRun() {
        int opIndex;
        while ( ! RunController.startFlag);

        while ( ! RunController.stopFlag) {
            double[] key = new double[this.dimension];
            opIndex = 0;
            int chooseOperation = randOp.nextInt(2);
            for (int i = 0; i < dimension; i ++) {
                key[i] = randKey.nextInt(keyRange);// Tools.randomInRange(randKey, 0, keyRange);
                // opIndex += (i == 0) ? key[i] : key[i] * keyRange;
                opIndex += key[i] * (int) Math.pow(keyRange, i);
            }
            // System.out.println(Arrays.toString(key)+" => "+opIndex + " op: " + chooseOperation);
            if (chooseOperation == 1) {
                try {
                    if (set.add(key, key)) {
                        numberOfAdd[opIndex] ++;
                    }
                    else if (set.remove(key)) {
                        numberOfRemove[opIndex] ++;
                    }
                }
                catch (DimensionLimitException e) {
                }
            }
            else {
                try {
                    if (set.remove(key)) {
                        numberOfRemove[opIndex] ++;
                    }
                    else if (set.add(key, key)) {
                        numberOfAdd[opIndex] ++;
                    }
                }
                catch (DimensionLimitException e) {
                }
            }
        }

        for (int i = 0; i < keyRange; i ++) {
            sanityAdds[threadId][i] += numberOfAdd[i];
            sanityRemoves[threadId][i] += numberOfRemove[i];
        }
    }

    @Override
    public void run() {
        if (testSanity) {
            sanityRun();
        }
        else {
            benchMarkRun();
        }
    }
}
