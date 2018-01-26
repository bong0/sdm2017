package de.tuda.sdm.dmdb.mapReduce.task.exercise;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.operator.MapperBase;
import de.tuda.sdm.dmdb.mapReduce.task.MapperTaskBase;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Defines what happens during the map-phase of a map-reduce job
 * Ie. implements the operator chains for a reduce-phase
 * The last operator in the chain writes to the output, ie. is used to populate the output
 * 
 * @author melhindi
 *
 */
public class MapperTask extends MapperTaskBase {

	public MapperTask(HeapTable input, HeapTable output, Class<? extends MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> mapperClass) {
		super(input,output, mapperClass);
	}

	@Override
	public void run() {
		// TODO: implement this method
		// read data from input (Remember: There is a special operator to read data from a Table)

		// instantiate the mapper-operator
		
		// process the input and write to the output
		
		// processing done

	}

}
