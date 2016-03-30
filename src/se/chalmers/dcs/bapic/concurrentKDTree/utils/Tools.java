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
package se.chalmers.dcs.bapic.concurrentKDTree.utils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author bapic
 */
enum RangeRelation {

    DISJOINT, OVERLAPS, ISCONTAINED
}

public class Tools {

    public static double distance(double[] a, double[] b) {
        return Math.sqrt(squaredDistance(a, b));
    }

    public static double squaredDistance(double[] a, double[] b) {
        double dist = 0;
        for (int i = 0; i < a.length;  ++ i) {
            double diff = (a[i] - b[i]);
            dist += diff * diff;
        }
        return Math.sqrt(dist);// dist;
    }

    public static boolean equals(int length, double[] a, double[] b) {
        for (int i = 0; i < length; i ++) {
            if (Double.doubleToLongBits(a[i]) != Double.doubleToLongBits(b[i])) {
                return false;
            }
        }
        return true;
    }

    public double[] closest(double[] t, double[] HRectMin, double[] HRectMax, int k) {

        double[] p = new double[k];

        for (int i = 0; i < k;  ++ i) {
            if (t[i] <= HRectMin[i]) {
                p[i] = HRectMin[i];
            }
            else if (t[i] >= HRectMax[i]) {
                p[i] = HRectMax[i];
            }
            else {
                p[i] = t[i];
            }
        }
        return p;
    }

    public static long getMemUsed() {
        long tot1 = Runtime.getRuntime().totalMemory();
        long free1 = Runtime.getRuntime().freeMemory();
        long used1 = tot1 - free1;
        return used1;
    }

    public static long printMemUsed(String txt, long prev) {
        long current = getMemUsed();
//        System.err.println(txt + ": " + (current - prev));
        return current - prev;
    }

    public static long printMemUsed(String txt, long prev, int n) {
        long current = getMemUsed();
//        System.out.println(txt + ": " + (current - prev) + "   per item: " + (current - prev) / n);
        return current - prev;
    }

    public static long cleanMem(long prevMemUsed) {
        long ret = 0;
        for (int i = 0; i < 5; i ++) {
            ret = printMemUsed("MemTree", prevMemUsed);
            System.gc();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public static long cleanMem(int N, long prevMemUsed) {
        long ret = 0;
        for (int i = 0; i < 5; i ++) {
            ret = printMemUsed("MemTree", prevMemUsed, N);
            System.gc();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    public static double randomInRange(Random random, double min, double max) {
        return (random.nextDouble() * (max-min)) + min;
    }

    public static boolean equals(double[] a, double[] a2) {
        if (a == a2) {
            return true;
        }
        int length = a.length;

        for (int i = 0; i < length; i ++) {
            if (Double.doubleToLongBits(a[i]) != Double.doubleToLongBits(a2[i])) {
                return false;
            }
        }
        return true;
    }

}
