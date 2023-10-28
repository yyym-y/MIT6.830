package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;
    private final OpIterator child;
    private final int tableId;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t; this.child = child; this.tableId = tableId;
        TupleDesc ttable = Database.getCatalog().getTupleDesc(tableId);
        if(! ttable.equals(child.getTupleDesc()))
            throw new DbException("TupleDesc no same");
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    private ArrayList<Tuple> tupleList = new ArrayList<>();
    private Iterator<Tuple> iterator;
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            count++;
            try {
                Database.getBufferPool().insertTuple(t, tableId, next);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(count));
        tupleList.add(tuple);
        iterator = tupleList.iterator();
        super.open();
    }

    public void close() {
        child.close();
        iterator = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        iterator = tupleList.iterator();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        //System.out.println(tupleList.get(0) + "----");
        if(iterator != null && iterator.hasNext())
            return iterator.next();
        return null;
    }

    private OpIterator[] children;

    @Override
    public OpIterator[] getChildren() {
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }
}
