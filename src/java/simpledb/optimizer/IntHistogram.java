package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private final int min;
    private final int max;
    private final int buckets;
    private final int each;
    private int[] tables;
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min; this.max = max;
        this.each = Math.max(1, (int)Math.ceil(((max - min + 1) * 1.0) / buckets));
        if(this.each == 1) this.buckets = max - min + 1;
        else this.buckets = buckets;
        this.tables = new int[this.buckets];
    }
    public int getPos(int v) {
        return  Math.abs(v - min) / each;
    }
    boolean change = false;
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        change = true;
        tables[getPos(v)] ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    private int[] Sum = null;
    public double rigthNum(int v) {
        return min + each * (getPos(v) + 1) - 1;
    }
    public double estimateSelectivity(Predicate.Op op, int v) {
        if(this.Sum == null || change) {
            Sum = new int[this.buckets]; Sum[0] = tables[0];
            for(int i = 1 ; i < this.buckets ; i++)
                Sum[i] = Sum[i - 1] + tables[i];
        }
        if(op.equals(Predicate.Op.EQUALS)) {
            if(v > max || v < min) return 0.0;
            return ((tables[getPos(v)] * 1.0) / each) / Sum[this.buckets - 1];
        }
        else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            if(v > max || v < min) return 1.0;
            return (Sum[buckets - 1] - (tables[getPos(v)] * 1.0) / each) / Sum[buckets - 1];
        }
        else if (op.equals(Predicate.Op.GREATER_THAN)) {
            if(v >= max) return 0.0;
            if(v < min) return 1.0;
            double len = (rigthNum(v) - v) * ((tables[getPos(v)] * 1.0) / each);
            len += (Sum[buckets - 1] - Sum[getPos(v)]);
            return len / Sum[buckets - 1];
        }else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if(v > max) return 0.0;
            if(v <= min) return 1.0;
            double len = (rigthNum(v) - v + 1) * ((tables[getPos(v)] * 1.0) / each);
            len += (Sum[buckets - 1] - Sum[getPos(v)]);
            return len / Sum[buckets - 1];
        } else if (op.equals(Predicate.Op.LESS_THAN)) {
            if(v > max) return 1.0;
            if(v <= min) return 0.0;
            double len = (each - (rigthNum(v) - v + 1)) * ((tables[getPos(v)] * 1.0) / each);
            len += getPos(v) == 0 ? 0 : Sum[getPos(v) - 1];
            return len / Sum[buckets - 1];
        } else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if(v >= max) return 1.0;
            if(v < min) return 0.0;
            double len = (each - (rigthNum(v) - v)) * ((tables[getPos(v)] * 1.0) / each);
            len += getPos(v) == 0 ? 0 : Sum[getPos(v) - 1];
            return len / Sum[buckets - 1];
        }
        return 1000.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
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
