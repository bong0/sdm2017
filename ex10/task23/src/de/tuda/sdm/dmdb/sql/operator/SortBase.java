package de.tuda.sdm.dmdb.sql.operator;

import java.util.Comparator;
import java.util.PriorityQueue;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

public abstract class SortBase extends UnaryOperator{
	protected PriorityQueue<AbstractRecord> sortedRecords;
	protected Comparator<AbstractRecord> recordComparator;
	protected boolean sorted = false;


	public SortBase(Operator child, Comparator<AbstractRecord> recordComparator) {
		super(child);
		this.recordComparator = recordComparator;
	}

}
