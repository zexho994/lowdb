package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import sun.applet.AppletResourceLoader;

import java.util.*;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] buckets;
    private final int min;
    private final int max;
    private final double width;
    private double ntups = 0.0;


    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = Math.max(1, (1 + max - min) / buckets);
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v < min || v > max) return;
        int index = getIndex(v);
        ntups++;
        buckets[index]++;
    }

    private int getIndex(int v) {
        return (int) Math.min(v - min / width, buckets.length - 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     *
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index = getIndex(v);
        if (op.equals(Predicate.Op.EQUALS)) {
            if (index < 0 || index >= buckets.length) return 0.0;
            return (buckets[index] / width) / ntups;
        } else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        } else if (op.equals(Predicate.Op.GREATER_THAN)) {
            if (v <= min) return 1.0;
            if (v >= max) return 0.0;
            int cnt = 0;
            for (int i = index + 1; i < buckets.length; i++) {
                cnt += buckets[i];
            }
            double bf = 1.0 * buckets[index] / ntups;
            double bPart = (index + 1) * width - v / width;
            return bf * bPart + 1.0 * cnt / ntups;
        } else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v)
                    - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        } else if (op.equals(Predicate.Op.LESS_THAN)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v) - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        } else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        }
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here


        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
