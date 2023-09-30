package simpledb.execution;

import simpledb.storage.IntField;
import simpledb.storage.Tuple;

public class AggOpera {
    public static abstract class Basic {
        public int afield;
        public Basic(int afield) {
            this.afield = afield;
        }
        public abstract int[] oper(int [] ori, Tuple tup);
        public abstract int[] init(Tuple tup);
        public int getNum(Tuple tup) {
            return ((IntField) tup.getField(this.afield)).getValue();
        }
    }

    public static class MAX extends Basic {
        public MAX(int afield) {
            super(afield);
        }
        @Override
        public int[] oper(int [] ori, Tuple tup) {
            int[] ans = new int[3];
            ans[0] = Math.max(getNum(tup), ori[0]);
            return ans;
        }
        @Override
        public int[] init(Tuple tup) {
            return new int[]{getNum(tup), 0, 0};
        }
    }

    public static class MIN extends Basic {
        public MIN(int afield) {
            super(afield);
        }
        @Override
        public int[] oper(int [] ori, Tuple tup) {
            int[] ans = new int[3];
            ans[0] = Math.min(getNum(tup), ori[0]);
            return ans;
        }
        @Override
        public int[] init(Tuple tup) {
            return new int[]{getNum(tup), 0, 0};
        }
    }

    public static class COUNT extends Basic {
        public COUNT(int afield) {
            super(afield);
        }
        @Override
        public int[] oper(int [] ori, Tuple tup) {
            int[] ans = new int[3];
            ans[0] = ori[0] + 1;
            return ans;
        }
        @Override
        public int[] init(Tuple tup) {
            return new int[]{1,0,0};
        }
    }

    public static class SUM extends Basic {
        public SUM(int afield) {
            super(afield);
        }
        @Override
        public int[] oper(int[] ori, Tuple tup) {
            int[] ans = new int[3];
            ans[0] = ori[0] + getNum(tup);
            return ans;
        }
        @Override
        public int[] init(Tuple tup) {
            return new int[]{getNum(tup), 0, 0};
        }
    }

    public static class AVG extends Basic {
        public AVG(int afield) {
            super(afield);
        }
        @Override
        public int[] oper(int[] ori, Tuple tup) {
            int[] ans = new int[3];
            ans[0] = ori[0] + 1; ans[1] = ori[1] + getNum(tup); ans[2] = ans[1] / ans[0];
            return ans;
        }
        @Override
        public int[] init(Tuple tup) {
            return new int[]{1, getNum(tup), getNum(tup)};
        }
    }
    public static class Tool {
        public static Basic getBasic(Aggregator.Op op, int afield) {
            if(op.equals(Aggregator.Op.MAX))
                return new MAX(afield);
            if(op.equals(Aggregator.Op.MIN))
                return new MIN(afield);
            if(op.equals(Aggregator.Op.SUM))
                return new SUM(afield);
            if(op.equals(Aggregator.Op.COUNT))
                return new COUNT(afield);
            if(op.equals(Aggregator.Op.AVG))
                return new AVG(afield);
            return null;
        }

        public static int getIndex(Aggregator.Op op) {
            if(op.equals(Aggregator.Op.MAX) || op.equals(Aggregator.Op.MIN)
                    || op.equals(Aggregator.Op.SUM) || op.equals(Aggregator.Op.COUNT))
                return 0;
            if(op.equals(Aggregator.Op.AVG))
                return 2;
            return -1;
        }
    }
}
