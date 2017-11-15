package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.Vector;

import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.ProjectionBase;
import de.tuda.sdm.dmdb.sql.operator.UnaryOperator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

@SuppressWarnings("unused")
public class Projection extends ProjectionBase {
	
	public Projection(Operator child, Vector<Integer> attributes) {
		super(child, attributes);
	}

	@Override
	public void open() {
		//TODO: implement this method
	}
	
	@Override
	public AbstractRecord next() {
		//TODO: implement this method
		return null;
	}
	
	@Override
	public void close() {
		//TODO: implement this method
	}
}
