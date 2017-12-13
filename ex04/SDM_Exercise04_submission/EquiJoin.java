package de.tuda.sdm.dmdb.sql.operator.exercise;

import de.tuda.sdm.dmdb.sql.operator.EquiJoinBase;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import java.util.Vector;

public class EquiJoin extends EquiJoinBase {
	
	public EquiJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	private AbstractRecord joinedRecordPrototype=null;
	Vector<AbstractRecord> rightRecords;


	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		rightRecords = new Vector<>();

		AbstractRecord rightRec;
		while((rightRec = rightChild.next()) != null){
			rightRecords.add(rightRec);
		}

	}

	@Override
	public AbstractRecord next() {

		// iterate over left table
		while((this.leftRecord = leftChild.next()) != null){

			for(AbstractRecord rightRecord : rightRecords){

				if(rightRecord.getValue(rightAtt).equals(this.leftRecord.getValue(leftAtt))){
					AbstractRecord tmpRec=null;

					if(this.joinedRecordPrototype == null){
						// init the prototype
						this.joinedRecordPrototype = new Record(
								leftRecord.getValues().length+rightRecord.getValues().length
						);
						tmpRec = this.joinedRecordPrototype;
					} else {
						tmpRec = this.joinedRecordPrototype.clone();
					}

					// copy left record into result
					for(int i=0; i<this.leftRecord.getValues().length; i++){
						tmpRec.setValue(i, leftRecord.getValue(i).clone());
					}
					// copy right record into result
					for(int i=0; i<rightRecord.getValues().length; i++){

						tmpRec.setValue(this.leftRecord.getValues().length+i, rightRecord.getValue(i).clone());
					}
					return tmpRec.clone();
				}
			}
		}
		return null;
	}
	
	@Override
	public void close() {
		this.leftRecord = null;
		this.joinedRecordPrototype = null;
		this.leftChild.close();
		this.rightChild.close();
	}

}
