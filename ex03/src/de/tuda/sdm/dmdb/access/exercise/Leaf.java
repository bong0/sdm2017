package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.LeafBase;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;

/**
 * Index leaf
 * @author cbinnig
 */
public class Leaf<T extends AbstractSQLValue> extends LeafBase<T>{

	/**
	 * Leaf constructor
	 * @param uniqueBPlusTree TODO
	 */
	public Leaf(UniqueBPlusTreeBase<T> uniqueBPlusTree){
		super(uniqueBPlusTree);
	}

	@Override
	public AbstractRecord lookup(T key) {

		AbstractRecord leafRecord = this.uniqueBPlusTree.getLeafRecPrototype().clone();

		// if fillgrade of leaf is 0 there can't be content to return
		if(this.indexPage.getNumRecords() == 0){
			return null;
		}

		int foundPos = this.binarySearch(key);
		this.indexPage.read(foundPos, leafRecord);

		System.out.println("fillgrade of leaf "+this.indexPage.getNumRecords());
		SQLInteger keyValue = (SQLInteger)leafRecord.getValue(UniqueBPlusTreeBase.KEY_POS);

		if(keyValue == key){
			return leafRecord;
		}

		return null;
	}
	
	@Override
	public boolean insert(T key, AbstractRecord record){

		if(lookup(key) != null){
			return false; // record with that key exists
		}

		this.indexPage.insert(record);

		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}