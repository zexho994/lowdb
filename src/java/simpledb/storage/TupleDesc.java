package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 *
 * @author zexho
 */
public class TupleDesc implements Serializable {

    private final List<TDItem> items;
    private final int size;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        return getSize() == tupleDesc.getSize() && items.equals(tupleDesc.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items, getSize());
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TDItem)) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType && fieldName.equals(tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // todo some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.items = new ArrayList<>(typeAr.length);
        IntStream.range(0, typeAr.length).mapToObj(i -> new TDItem(typeAr[i], fieldAr[i])).forEach(this.items::add);
        size = this.items.stream().map(i -> i.fieldType.getLen()).reduce(Integer::sum).get();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.items = Arrays.stream(typeAr).map(type -> new TDItem(type, "")).collect(Collectors.toList());
        size = this.items.stream().map(i -> i.fieldType.getLen()).reduce(Integer::sum).get();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     *
     * @return the name of the ith field
     *
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return this.items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     *
     * @return the type of the ith field
     *
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return this.items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     *
     * @return the index of the field that is first to have the given name.
     *
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.items.size(); i++) {
            TDItem tdItem = this.items.get(i);
            if (tdItem.fieldName == name) {
                return i;
            }
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException(String.format("name = %s", name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return this.size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     *
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        final Type[] newTypeArray = new Type[td1.numFields() + td2.numFields()];
        final String[] newNameArray = new String[td1.numFields() + td2.numFields()];
        int idx = 0;
        while (idx < td1.numFields()) {
            newTypeArray[idx] = td1.getFieldType(idx);
            newNameArray[idx] = td1.getFieldName(idx++);
        }
        int i = 0;
        while (i < td2.numFields()) {
            newNameArray[idx] = td2.getFieldName(i);
            newTypeArray[idx++] = td2.getFieldType(i++);
        }

        return new TupleDesc(newTypeArray, newNameArray);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder str = new StringBuilder();
        IntStream.range(0, this.numFields())
                .mapToObj(i -> String.format("%s(%s),", this.getFieldType(i), this.getFieldName(i)))
                .forEach(str::append);
        return str.substring(0, str.length() - 1);
    }
}
