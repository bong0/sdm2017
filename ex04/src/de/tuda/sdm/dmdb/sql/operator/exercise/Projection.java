package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.Vector;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.ProjectionBase;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;

@SuppressWarnings("unused")
public class Projection extends ProjectionBase {
	
	public Projection(Operator child, Vector<Integer> attributes) {
		super(child, attributes);
	}

	private AbstractRecord prototypeOutputRec=null;
	@Override
	public void open() {
		child.open();
	}
	
	@Override
	public AbstractRecord next() {

		AbstractRecord inputRecord = child.next();
		if(inputRecord == null) return null;
		// init prototype


		inputRecord.keepValues(attributes);

		return inputRecord.clone();
	}
	
	@Override
	public void close() {
		child.close();
	}
}
