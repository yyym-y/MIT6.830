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
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(TDItem td) {
            if(!(td.fieldType.equals(this.fieldType))) return false;
            if(td.fieldName == null && this.fieldName == null)
                return true;
            return td.fieldName.equals(this.fieldName);
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
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

    private ArrayList<TDItem> list = new ArrayList<>();
    private HashMap<String, Integer> map = new HashMap<>();

    public ArrayList<TDItem> getList() {
        return list;
    }

    public HashMap<String, Integer> getMap() {
        return map;
    }

    public int getListSize() {
        return list.size();
    }

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for(int temp = 0 ; temp < typeAr.length ; temp++) {
            list.add(new TDItem(typeAr[temp], fieldAr[temp]));
            map.put(fieldAr[temp], temp);
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

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return list.size();
    }

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
        Type[] newTypeList = new Type[td1.getListSize() + td2.getListSize()];
        String[] newNameList = new String[td1.getListSize() + td2.getListSize()];
        ArrayList<TDItem> list1 = td1.getList();
        ArrayList<TDItem> list2 = td2.getList();
        int pos = 0;
        for(int i = 0 ; i < td1.getListSize() ; i++) {
            newTypeList[pos] = list1.get(i).fieldType;
            newNameList[pos++] = list1.get(i).fieldName;
        }
        for(int i = 0 ; i < td2.getListSize() ; i++) {
            newTypeList[pos] = list2.get(i).fieldType;
            newNameList[pos++] = list2.get(i).fieldName;
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

    public boolean equals(Object o) {
        if(! (o instanceof TupleDesc))
            return false;
        TupleDesc give = (TupleDesc) o;
        if(give.getListSize() != this.getListSize())
            return false;
        ArrayList<TDItem> list2 = give.getList();
        for(int i = 0 ; i < list2.size() ; i++) {
            if(! (list.get(i).equals(list2.get(i))))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder form = null;
        for(int i = 0 ; i < list.size() ; i++) {
            form.append(",").append(list.get(i).toString());
        }
        return form.substring(1);
    }
}
