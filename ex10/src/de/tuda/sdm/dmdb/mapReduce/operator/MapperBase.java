package de.tuda.sdm.dmdb.mapReduce.operator;

import java.util.Queue;

import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * similar to: https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Mapper.java
 * Base Class for implementing the Mapper-Operator
 * Hides complexity from students and provides helper methods
 * 
 * Maps input key/value pairs to a set of intermediate key/value pairs.  
 * 
 * Maps are the individual tasks which transform input records into a 
 * intermediate records. A given input pair may map to zero or 
 * many output pairs. Output pairs are normally written to the nextList member
 * 
 * @author melhindi
 *
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 */
public abstract class MapperBase<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue> extends MapReduceOperator{

	/**
	 * Called once for each keyValueRecord (key/value pair) in the input (split). Most applications
	 * should override this, but the default is the identity function. (This assumes KEYIN and KEYOUT/VALUEIN and VALUEOUT are of the same type)
	 */
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, 
			Queue<AbstractRecord> outList) {
		AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
		newRecord.setValue(MapReduceOperator.KEY_COLUMN, (KEYOUT) key);
		newRecord.setValue(MapReduceOperator.VALUE_COLUMN,  (VALUEOUT)  value);

		outList.add(newRecord);
	}

}
