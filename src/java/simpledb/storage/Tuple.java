package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    // 记录这个 Tuple 的存放信息（第几个Page， Page中的第几个）
    private RecordId recordId;
    // 存放表头信息
    private TupleDesc td;

    private ArrayList<Field> list = new ArrayList<>();


    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    // 构造函数，注意Field的类型要和TupleDesc的类型要匹配
    public Tuple(TupleDesc td) {
        this.td = td;
        for(int i = 0 ; i < td.numFields() ; i++) {
            if(td.getFieldType(i) == Type.INT_TYPE)
                list.add(new IntField(0));
            else if(td.getFieldType(i) == Type.STRING_TYPE)
                list.add(new StringField("", Type.STRING_LEN));
        }
    }
    // 获取 TupleDesc
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }
    // 获取 RecordId
    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }
    // 重置 RecordId
    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }
    //更改之指定位置的Field
    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if(i < 0 || i >= list.size())
            throw new NoSuchElementException();
        list.set(i, f);
    }
    // 获取指定位置的 Field
    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if(i >= list.size() || i < 0)
            throw new NoSuchElementException();
        return list.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        String s = "";
        for(int i = 0 ; i < list.size() ; i ++) {
            s += "\t" + list.get(i).toString();
        }
        return s.substring(1);
    }
    //返回一个Field的迭代器
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return list.iterator();
    }
    // 重置表头信息
    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
    }
}
