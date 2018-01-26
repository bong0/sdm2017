package de.tuda.sdm.dmdb.mapReduce.operator.exercise;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
		this.nextList = new ConcurrentLinkedQueue<AbstractRecord>(); // from MapreduceOperator
		this.lastRecord = null;
		this.child.open();
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractRecord next() {
		// TODO: implement this method
		// this method has to prepare the input to the reduce function
		// it also returns the result of the reduce function as next
		// Implement the grouping of mapper-outputs here 
		// You can assume that the input to the reducer is sorted. This makes the grouping operation easier. Keep this in mind if you write your own tests (make sure that input to reducer is sorted)

		// first, clear cached records that already fell out of reduction so we can do the next reduction
		if(!this.nextList.isEmpty()){
			return this.nextList.poll();
		}

		Queue<VALUEIN> valueQueue = new LinkedList<>();

		// retrieve next input record
		AbstractRecord currentRecord = null;


		while(true){
			currentRecord = this.child.next();

			boolean key_switched = false;
			if(lastRecord == null && currentRecord == null){
				return null; // no input was made, quit operator right away
			}
			else if(currentRecord == null){
				key_switched=true; // this is the last item, we need to force a reduction!
			}
			else if(!this.lastRecord.getValue(KEY_COLUMN).equals(currentRecord.getValue(KEY_COLUMN))){
				key_switched = true; // the key changed, trigger a reduction
			}

			if(key_switched) {
				System.out.println("key change doing red");

				this.reduce((KEYIN)this.lastRecord.getValue(KEY_COLUMN), valueQueue, this.nextList);

				this.lastRecord = currentRecord; // now that reduction is done, switch lastRecord to the new group
				if(!this.nextList.isEmpty()) {
					return this.nextList.poll();
				}
			}


			this.lastRecord = currentRecord; // update LastRecord
			valueQueue.add((VALUEIN) currentRecord.getValue(VALUE_COLUMN));
		}
		// invoke the reduce function on the input and pass in this.nextList to cache the output pairs there

	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
		this.child.close();
		this.nextList.clear();
	}

}
