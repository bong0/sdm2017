package de.tuda.sdm.dmdb.mapReduce.task.exercise;

import java.util.Comparator;
import java.util.Map;
import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.sql.operator.exercise.Sort;
import de.tuda.sdm.dmdb.mapReduce.operator.MapperBase;
import de.tuda.sdm.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.sdm.dmdb.mapReduce.task.SinglePhaseTaskBase;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Defines what happens during the execution a map-reduce task
 * Ie. implements the operator chains for a complete map-reduce task
 * We assume the same number of mappers and reducers (no need to change hashFunction of shuffle)
 * The Operator chain that this task implements is: Scan->Mapper->Shuffle->Sort->Reducer
 * The last operator in the chain writes to the output, ie. is used to populate the output
 * 
 * @author melhindi
 *
 */

/*
This is the implementation of task pipeline on one node (each node executes the same pipeline)
 */
public class SinglePhaseTask extends SinglePhaseTaskBase {

	public SinglePhaseTask(HeapTable input, HeapTable output, int nodeId, Map<Integer, String> nodeMap, int partitionColumn,
			Class<? extends MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> mapperClass,
		   Class<? extends ReducerBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> reducerClass) {
		super(input,output, nodeId, nodeMap, partitionColumn, mapperClass, reducerClass);
	}

	@Override
	public void run() {

		// TODO: implement this method
		// read data from input (Remember: There is a special operator to read data from a Table)

		//Scan->Mapper->Shuffle->Sort->Reducer
		try {
			MapperBase mapper = this.mapperClass.newInstance();
			mapper.setChild(new TableScan(input));

			Shuffle shuffle = new Shuffle(mapper, nodeId, nodeMap, this.port, partitionColumn);
			Sort sort = new Sort(shuffle, this.recordComparator);
			ReducerBase reducer = this.reducerClass.newInstance();
			reducer.setChild(sort);

			reducer.open();

			AbstractRecord reducedRec = null;
			while((reducedRec = reducer.next())!=null){
				System.out.println("node"+this.nodeId+" puts rec " +reducedRec);
				this.output.insert(reducedRec);
			}
			reducer.close();


		}catch(Exception e){
			System.err.println("FATAL: an exception occurred "+e.getMessage());
			e.printStackTrace();
		}
		// define/instantiate the required operators

		// process the input and write to the output

		// processing done

	}

}
