package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.Comparator;
import java.util.PriorityQueue;

import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SortBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

public class Sort extends SortBase{


	public Sort(Operator child, Comparator<AbstractRecord> recordComparator) {
		super(child, recordComparator);
	}

	@Override
	public void open() {
		// TODO: implement this method
		// make sure to initialize the required (inherited) member variables
		this.child.open();
		this.sortedRecords = new PriorityQueue<>(this.recordComparator);
		sorted = false;
	}

	@Override
	public AbstractRecord next() {
		// block and sort when required
		// blocking part
		// sort, by adding to PriorityQueue

		// readout sorted elements if finished
		AbstractRecord nextRec = null;
		System.out.println("sort got input "+nextRec);
		while(true) {
			if(sorted == false) {
				nextRec = this.child.next();
				if (nextRec == null) {
					this.sorted = true;
					System.out.println("sort outputs "+this.sortedRecords.peek());
					return this.sortedRecords.poll();
				}
			} else {
				System.out.println("sort outputs "+this.sortedRecords.peek());
				return this.sortedRecords.poll();
			}

			this.sortedRecords.add(nextRec);
		}

	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
		this.child.close();
		this.sortedRecords.clear();
	}

}
