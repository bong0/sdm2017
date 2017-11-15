package de.tuda.sdm.dmdb.sql.operator;

import de.tuda.sdm.dmdb.sql.operator.BinaryOperator;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;

public abstract class EquiJoinBase extends BinaryOperator {
	protected int leftAtt=0;
	protected int rightAtt=0;
	
	protected AbstractRecord leftRecord = null;
	
	public EquiJoinBase(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild);
		this.leftAtt = leftAtt;
		this.rightAtt = rightAtt;
	}
}
