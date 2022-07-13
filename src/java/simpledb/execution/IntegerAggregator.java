package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

import static simpledb.execution.Aggregator.Op.MIN;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldIdx;
    private final Type groupByFieldType;
    private final int aggregateFieldIdx;
    private final Op op;

    /**
     * val: [occurrences,sum]
     */
    private final Map<Field, Integer[]> countMap;
    private final Map<Field, Integer> groupingMap;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldIdx = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIdx = afield;
        this.op = what;
        if (gbfield != NO_GROUPING) {
            this.groupingMap = new HashMap<>();
            this.countMap = new HashMap<>();
        } else {
            this.groupingMap = null;
            this.countMap = null;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (groupByFieldType == null) {
            return;
        }

        // some code goes here
        if (tupleDesc == null) {
            tupleDesc = new TupleDesc(new Type[]{tup.getTupleDesc().getFieldType(groupByFieldIdx), Type.INT_TYPE});
        }
        Field gfi = tup.getField(groupByFieldIdx);
        IntField intField = (IntField) tup.getField(this.aggregateFieldIdx);
        Integer oldVal;

        switch (op) {
            case MIN:
                oldVal = groupingMap.getOrDefault(gfi, Integer.MAX_VALUE);
                if (intField.getValue() < oldVal) {
                    groupingMap.put(gfi, intField.getValue());
                }
                break;
            case MAX:
                oldVal = groupingMap.getOrDefault(gfi, Integer.MIN_VALUE);
                if (intField.getValue() > oldVal) {
                    groupingMap.put(gfi, intField.getValue());
                }
                break;
            case SUM:
                oldVal = groupingMap.getOrDefault(gfi, 0);
                groupingMap.put(gfi, oldVal + intField.getValue());
                break;
            case AVG:
                Integer[] count = countMap.getOrDefault(gfi, new Integer[]{0, 0});
                count[0]++;
                count[1] += intField.getValue();
                groupingMap.put(gfi, count[1] / count[0]);
                countMap.put(gfi, count);
                break;
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
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
                tuple.setField(aggregateFieldIdx, new IntField(v));
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
