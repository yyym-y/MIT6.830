package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class JoinTool {
    public static abstract class JoinMethod {
        private final OpIterator child1;
        private final OpIterator child2;
        private final TupleDesc td;
        private final JoinPredicate p;
        private Iterator<Tuple> item;
        private ArrayList<Tuple> tuples = new ArrayList<>();

        public JoinMethod(OpIterator child1, OpIterator child2, JoinPredicate p) {
            this.child1 = child1; this.child2 = child2; this.p = p;
            td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
            try {
                getItem();
            } catch (TransactionAbortedException | DbException e) {
                throw new RuntimeException(e);
            }
        }
        private Tuple merge(Tuple one, Tuple two) {
            Tuple tem = new Tuple(td);
            int pos = 0;
            for(int i = 0 ; i < one.getTupleDesc().numFields() ; i++)
                tem.setField(pos ++, one.getField(i));
            for(int i = 0 ; i < two.getTupleDesc().numFields() ; i++)
                tem.setField(pos ++, two.getField(i));
            return tem;
        }
        public Tuple next() {
            if(item.hasNext()) return item.next();
            return null;
        }
        public void rewind() {
            item = tuples.iterator();
        }
        public abstract void getItem() throws TransactionAbortedException, DbException;
    }

    public static class LoopJoin extends JoinMethod {
        public LoopJoin(OpIterator child1, OpIterator child2, JoinPredicate p) {
            super(child1, child2, p);
        }
        @Override
        public void getItem() throws TransactionAbortedException, DbException {
            super.child1.open(); super.child2.open();
            while(super.child1.hasNext()) {
                Tuple one = super.child1.next();
                while(super.child2.hasNext()) {
                    Tuple two = super.child2.next();
                    if (super.p.filter(one, two))
                        super.tuples.add(super.merge(one, two));
                }
                super.child2.rewind();
            }
            super.item = super.tuples.iterator();
        }
    }

    public static class HashJoin extends JoinMethod {
        public HashJoin(OpIterator child1, OpIterator child2, JoinPredicate p) {
            super(child1, child2, p);
        }
        @Override
        public void getItem() throws TransactionAbortedException, DbException {
            super.child1.open(); super.child2.open();
            HashMap<Field, ArrayList<Tuple>> map = new HashMap<>();
            while (super.child2.hasNext()) {
                Tuple two = super.child2.next();
                Field field = two.getField(super.p.getField2());
                map.computeIfAbsent(field, k -> new ArrayList<>());
                map.get(field).add(two);
            }
            while (super.child1.hasNext()) {
                Tuple one = super.child1.next();
                Field field = one.getField(super.p.getField1());
                if(map.get(field) == null)
                    continue;
                for(Tuple pr : map.get(field)) {
                    if(super.p.filter(one, pr))
                        super.tuples.add(super.merge(one, pr));
                }
            }
            super.item = super.tuples.iterator();
        }
    }

    public static class MergeSortJoin extends JoinMethod {
        public MergeSortJoin(OpIterator child1, OpIterator child2, JoinPredicate p) {
            super(child1, child2, p);
        }
        public void mergeSort(ArrayList<Tuple> arr, int left, int right, int ind) {
            int mid = (left + right) >> 1;
            if(left != right) {
                mergeSort(arr, left, mid, ind);
                mergeSort(arr, mid + 1, right, ind);
            }
            Tuple[] tem = new Tuple[right - left + 1];
            int pos1 = left, pos2 = mid + 1, pos3 = 0;
            while(pos1 <= mid && pos2 <= right) {
                if(arr.get(pos1).getField(ind).compare(Predicate.Op.LESS_THAN, arr.get(pos2).getField(ind)))
                    tem[pos3 ++] = arr.get(pos1++);
                else
                    tem[pos3 ++] = arr.get(pos2++);
            }
            while (pos1 <= mid) tem[pos3++] = arr.get(pos1++);
            while (pos2 <= right) tem[pos3++] = arr.get(pos2++);
            pos3 = 0;
            for(int i = left ; i <= right ; i++)
                arr.set(i, tem[pos3 ++]);
        }
        public ArrayList<Tuple> getSort(OpIterator it, int ind) throws TransactionAbortedException, DbException {
            ArrayList<Tuple> arr = new ArrayList<>(); it.open();
            while (it.hasNext()) arr.add(it.next());
            mergeSort(arr, 0, arr.size() - 1, ind);
            return arr;
        }
        @Override
        public void getItem() throws TransactionAbortedException, DbException {
            ArrayList<Tuple> first = getSort(super.child1, super.p.getField1());
            ArrayList<Tuple> second = getSort(super.child2, super.p.getField2());
            int pos = 0; Predicate.Op op = super.p.getOperator();
            for (Tuple tuple : first) {
                if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                    while (pos < second.size() && super.p.filter(tuple, second.get(pos)))
                        pos++;
                    for (Tuple value : second) super.tuples.add(super.merge(tuple, value));
                } else if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                    while (pos < second.size() && ! super.p.filter(tuple, second.get(pos)))
                        pos++;
                    for (Tuple value : second) super.tuples.add(super.merge(tuple, value));
                }
            }
            super.item = super.tuples.iterator();
        }
    }
    public static class Tool {
        public static JoinMethod getMethod(OpIterator child1, OpIterator child2, JoinPredicate p) {
            if(p.getOperator().equals(Predicate.Op.EQUALS))
                return new HashJoin(child1, child2, p);
            if(child1.getTupleDesc().getFieldType(p.getField1()) != Type.INT_TYPE ||
               child2.getTupleDesc().getFieldType(p.getField2()) != Type.INT_TYPE)
                return new LoopJoin(child1, child2, p);
            return new MergeSortJoin(child1, child2, p);
        }
    }
}
