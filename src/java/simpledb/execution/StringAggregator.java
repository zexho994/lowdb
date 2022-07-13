package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int groupByFieldIdx;
    private final Type groupByFieldType;
    private final int aggregationFieldIdx;
    private final Op op;

    /**
     * val: [occurrences,sum]
     */
    private final Map<Field, Integer> groupingMap;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     *
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldIdx = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregationFieldIdx = afield;
        this.op = what;
        this.groupingMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (groupByFieldIdx == -1) {
            return;
        }
        if (tupleDesc == null) {
            tupleDesc = new TupleDesc(new Type[]{tup.getTupleDesc().getFieldType(groupByFieldIdx), Type.INT_TYPE});
        }

        Field gfi = tup.getField(groupByFieldIdx);
        switch (op) {
            case COUNT:
                groupingMap.put(gfi, groupingMap.getOrDefault(gfi, 0) + 1);
                break;
            default:
                throw new RuntimeException("op not suppose");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private Iterator<Field> keyIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                keyIterator = groupingMap.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (keyIterator == null) {
                    throw new DbException("OpIterator not open");
                }
                return keyIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (keyIterator == null) {
                    throw new DbException("Operator not open");
                }
                Field field = keyIterator.next();
                Integer v = groupingMap.get(field);
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(aggregationFieldIdx, new IntField(v));
                tuple.setField(groupByFieldIdx, field);
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {
                keyIterator = null;
            }
        };
    }

}
