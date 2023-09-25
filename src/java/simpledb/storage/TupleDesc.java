package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    // 这个内部类用来表示TuplesDesc每一列的相关信息
    public static class TDItem implements Serializable {

        // serialVersionUID用于序列化，可以不管
        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * 用来表示这一列所存放的数据类型，即存的是数字还是字符串
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * 这一列所表示的含义(即名字)
         * */
        public final String fieldName;
        // 内部类构造函数
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        // 重写toString方法，方便输出
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        // 判断两个TDitem是否相等，方便后面判断两个TuplesDesc时候相等
        public boolean equals(TDItem td) {
            if(!(td.fieldType.equals(this.fieldType))) return false;
            if(td.fieldName == null && this.fieldName == null)
                return true;
            return td.fieldName.equals(this.fieldName);
        }
        // 快速获得TDitem的哈希值
        public int hashcode() {
            if(fieldName != null)
                return this.fieldName.hashCode() + fieldType.hashCode();
            return fieldType.hashCode();
        }
    }

    /**
     * 返回一个TDitem的迭代器，以后会有用
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return list.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    // 用来存放 TDitem
    private ArrayList<TDItem> list = new ArrayList<>();
    // 建立一个Hash映射表， 这样可以通过名字快速的找到这一列在数组中的索引
    private HashMap<String, Integer> map = new HashMap<>();
    // 这个TuplesDesc 的哈希值
    private int code = 0;
    // 通过这个函数快速获得这个表头的所有 TDitem信息
    public ArrayList<TDItem> getList() {
        return list;
    }
    // 获得这张表的哈希映射表
    public HashMap<String, Integer> getMap() {
        return map;
    }

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for(int temp = 0 ; temp < typeAr.length ; temp++) {
            TDItem td = new TDItem(typeAr[temp], fieldAr[temp]);
            list.add(td);
            map.put(fieldAr[temp], temp);
            code += td.hashcode();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for(int temp = 0 ; temp < typeAr.length ; temp++) {
            list.add(new TDItem(typeAr[temp], null));
        }
    }
    // 返回这个表头一共有多少的TDitem
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return list.size();
    }
    // 获取第 i 个 TDitem 的名字
    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i > list.size())
            throw new NoSuchElementException();
        return list.get(i).fieldName;
    }
    // 获取第 i 个 TDitem 的类型
    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i < 0 || i > list.size())
            throw new NoSuchElementException();
        return list.get(i).fieldType;
    }
    // 获取这个名字对应的索引
    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        Integer pos = map.get(name);
        if(pos == null)
            throw new NoSuchElementException();
        return pos;
    }
    // 获取这个Tuples所对应Tuple的字节大小
    // 我们阅读Type的源码可以得到两种类型的字节大小，所以只需要遍历累加即可
    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int temp = 0 ; temp < list.size() ; temp++) {
            size += list.get(temp).fieldType.getLen();
        }
        return size;
    }

    // 合并两个 TupleDesc 并返回合并后的 TupleDesc
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] newTypeList = new Type[td1.numFields() + td2.numFields()];
        String[] newNameList = new String[td1.numFields() + td2.numFields()];
        int pos = 0;
        for(int i = 0 ; i < td1.numFields() ; i++) {
            newTypeList[pos] = td1.getFieldType(i);
            newNameList[pos++] = td1.getFieldName(i);
        }
        for(int i = 0 ; i < td2.numFields() ; i++) {
            newTypeList[pos] = td2.getFieldType(i);
            newNameList[pos++] = td2.getFieldName(i);
        }
        return new TupleDesc(newTypeList, newNameList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    // 判断输入是否和本实例一样
    public boolean equals(Object o) {
        if(o == null) return false;
        if(o.getClass() != getClass()) return false;
        TupleDesc give = (TupleDesc) o;
        if(give.numFields() != this.numFields())
            return false;
        ArrayList<TDItem> list2 = give.getList();
        for(int i = 0 ; i < numFields() ; i++) {
            if(! (list.get(i).equals(list2.get(i))))
                return false;
        }
        return true;
    }
    // 返回哈希值
    public int hashCode() {
        return code;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder form = new StringBuilder();
        for(int i = 0 ; i < list.size() ; i++) {
            form.append(",").append(list.get(i).toString());
        }
        return form.substring(1);
    }
}
