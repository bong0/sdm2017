package de.tuda.sdm.dmdb.mapReduce.task.exercise;

import java.util.Map;
import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.task.ShuffleSortTaskBase;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.sql.operator.exercise.Sort;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

/**
 * Defines what happens during the shuffle&sort-phase of a map-reduce job
 * Ie. implements the operator chains for a shuffle&sort-phase
 * The last operator in the chain writes to the output, ie. is used to populate the output
 * 
 * @author melhindi
 *
 */
public class ShuffleSortTask extends ShuffleSortTaskBase {
	
	public ShuffleSortTask(HeapTable input, HeapTable output, int nodeId, Map<Integer, String> nodeMap, int partitionColumn, int numReducers) {
		super(input,output, nodeId, nodeMap, partitionColumn, numReducers);
	}
	
	@Override
	public void run() {
		// TODO: implement this method
		// read data from input (Remember: There is a special operator to read data from a Table)

		// define the shuffle-operator, use shuffleOperator.setHashFunction(this.numReducers); to change the hasfunction and account for a different number of reducers
		
		// define the sort operator, the base class already defines the comparator that you can use

		// process the input and write to the output

		// processing done

	}

}
