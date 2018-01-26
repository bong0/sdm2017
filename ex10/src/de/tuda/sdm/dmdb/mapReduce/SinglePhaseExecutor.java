package de.tuda.sdm.dmdb.mapReduce;

import java.util.concurrent.Future;

import de.tuda.sdm.dmdb.mapReduce.task.exercise.SinglePhaseTask;

/**
 * Executes map-reduce job as one phase
 * Each node will execute map,shuffle&sort,reduce and write out the result
 * @author melhindi
 *
 */
public class SinglePhaseExecutor extends MapReduceExecutor{

	@Override
	public void submit(Job job) {
		init(job); // prepare execution

		// tasks are added to the tasks memeber to be able to track progress
		// the call to waitForCompletion() (blocking) removes the finished tasks from the tasks member

		// sort&shuffle phase
		for (int i = 0; i < this.numMappers; i++) {
			Future<?> future = cluster.submit(new SinglePhaseTask(job.getInputs().get(i), job.getOutputs().get(i), i, this.nodeMap, job.getConfiguration().getPartitionColumn(), job.getMapperClass(), job.getReducerClass()));
			tasks.add(future);
		}

		// wait for phase to finish
		this.waitForCompletion();

		// make sure to updated state (ie, 'completed' member) to reflect that execution has finished
		this.completed = true;
	}


}
