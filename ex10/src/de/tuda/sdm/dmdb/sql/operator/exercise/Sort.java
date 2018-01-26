package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.Comparator;
import java.util.PriorityQueue;

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
	}

	@Override
	public AbstractRecord next() {
		// block and sort when required
		// blocking part
		// sort, by adding to PriorityQueue

		return null;
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
	}

}
