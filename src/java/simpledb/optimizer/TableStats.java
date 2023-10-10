package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    public class Histogram {
        private Object obj;
        public Histogram(int buckets, int min, int max) {
            obj = new IntHistogram(buckets, min, max);
        }
        public Histogram(int buckets) {
            obj = new StringHistogram(buckets);
        }
        public void addValues(int v) {
            ((IntHistogram) obj).addValue(v);
        }
        public void addValues(String v) {
            ((StringHistogram) obj).addValue(v);
        }
        public double estimateSelectivity(Predicate.Op op, int v) {
            return ((IntHistogram) obj).estimateSelectivity(op, v);
        }
        public double estimateSelectivity(Predicate.Op op, String v) {
            return ((StringHistogram) obj).estimateSelectivity(op, v);
        }
    }
    private ArrayList<Histogram> each;
    private int[] Max;
    private int[] Min;
    private double scanCost;
    private int numTuples = 0;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        each = new ArrayList<>(tupleDesc.numFields());
        Max = new int[tupleDesc.numFields()]; Min = new int[tupleDesc.numFields()];
        for(int i = 0 ; i < tupleDesc.numFields() ; i++) {
            Max[i] = Integer.MIN_VALUE; Min[i] = Integer.MAX_VALUE;
        }
        DbFileIterator tuples = dbFile.iterator(new TransactionId());
        try {
            tuples.open();
            while (tuples.hasNext()) {
                Tuple next = tuples.next(); numTuples ++;
                for(int i = 0 ; i < tupleDesc.numFields() ; i++) {
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        Max[i] = Math.max(Max[i], ((IntField) next.getField(i)).getValue());
                        Min[i] = Math.min(Min[i], ((IntField) next.getField(i)).getValue());
                    }
                }
            }
            for(int i = 0 ; i < tupleDesc.numFields() ; i++) {
                if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE))
                    each.add(new Histogram((Max[i] - Min[i]) / 10, Min[i], Max[i]));
                else
                    each.add(new Histogram(10));
            }
            tuples.rewind();
            while (tuples.hasNext()) {
                Tuple next = tuples.next();
                for(int i = 0 ; i < tupleDesc.numFields() ; i++) {
                    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE))
                        each.get(i).addValues(((IntField) next.getField(i)).getValue());
                    else
                        each.get(i).addValues(((StringField) next.getField(i)).getValue());
                }
            }
            this.scanCost = ((HeapFile) dbFile).numPages() * ioCostPerPage;
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return scanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // skip 大家都不xie....
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(constant.getType().equals(Type.INT_TYPE))
            return each.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        else
            return each.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
