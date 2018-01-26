package de.tuda.sdm.dmdb.mapReduce.operator.exercise;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * similar to https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Reducer.java
 * 
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 * 
 * A Reducer performs three primary tasks:
 * 	1) Read sorted input from child
 *  2) Group input from child (ie., prepare input to reduce function)
 *  3) Invoke the reduce() method on the prepared input to generate new output pairs
 * 
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
 * 
 * 
 * @author melhindi
 *
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 */
public class Reducer<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue> extends ReducerBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	
	@Override
	public void open() {
		// TODO: implement this method
		// make sure to initialize ALL (inherited) member variables

	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractRecord next() {
		// TODO: implement this method
		// this method has to prepare the input to the reduce function
		// it also returns the result of the reduce function as next
		// Implement the grouping of mapper-outputs here 
		// You can assume that the input to the reducer is sorted. This makes the grouping operation easier. Keep this in mind if you write your own tests (make sure that input to reducer is sorted)

		// retrieve next input record

		// prepare input for the reduce function (group by)

		// invoke the reduce function on the input and pass in this.nextList to cache the output pairs there

		return null;

	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()

	}

}
