package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.HashMap;

import de.tuda.sdm.dmdb.sql.operator.HashJoinBase;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

public class HashJoin extends HashJoinBase {

	public HashJoin(Operator leftChild, Operator rightChild, int leftAtt, int rightAtt) {
		super(leftChild, rightChild, leftAtt, rightAtt);
	}

	@Override
	public void open() {

		this.leftChild.open();
		this.rightChild.open();

		// build hashmap
		this.hashMap = new HashMap<AbstractSQLValue, AbstractRecord>();

		AbstractRecord curRightRec;
		while((curRightRec = this.rightChild.next()) != null){
			this.hashMap.put(curRightRec.getValue(rightAtt), curRightRec);
		}
		this.rightChild.close();
	}

	@Override
	public AbstractRecord next() {
		// probe HashTable and return next record
		this.leftRecord = this.leftChild.next();
		if(this.leftRecord != null){
			AbstractRecord possibleJoinPartner = this.hashMap.get(this.leftRecord.getValue(leftAtt));
			if(possibleJoinPartner.getValue(rightAtt).equals(this.leftRecord.getValue(leftAtt))){
				// we may join since the keys were really identical
				return this.leftRecord.append(possibleJoinPartner);
			}
		}

		return null;
	}

	@Override
	public void close() {
		this.leftChild.close();
	}
}
