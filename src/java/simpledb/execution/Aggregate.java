package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final Aggregator agg;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child; this.afield = afield;
        this.gfield = gfield; this.aop = aop;
        if(this.child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE))
            agg = new IntegerAggregator(this.gfield, Type.INT_TYPE, this.afield, this.aop);
        else
            agg = new StringAggregator(this.gfield, Type.STRING_TYPE, this.afield, this.aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if(this.gfield == Aggregator.NO_GROUPING)
            return null;
        return this.child.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }



    private OpIterator item;
    private boolean added = false;
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext() && ! added) {
            agg.mergeTupleIntoGroup(child.next());
        }
        added = true; child.rewind();
        item = agg.iterator();
        item.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(item.hasNext())
            return item.next();
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        item = agg.iterator();
        item.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(afield)},
                                 new String[]{child.getTupleDesc().getFieldName(afield)});
        }else {
            return new TupleDesc(new Type[]{child.getTupleDesc().getFieldType(gfield),
                                            child.getTupleDesc().getFieldType(afield)},
                                 new String[]{child.getTupleDesc().getFieldName(gfield),
                                              child.getTupleDesc().getFieldName(afield)});
        }
    }

    public void close() {
        super.close();
    }

    private OpIterator[] children;

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }

}
