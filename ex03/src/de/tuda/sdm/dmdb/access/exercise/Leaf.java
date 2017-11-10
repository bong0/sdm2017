package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractIndexElement;
import de.tuda.sdm.dmdb.access.LeafBase;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.access.UniqueBPlusTreeBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;

import java.sql.RowId;

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

	/*
   Leaf lookup has a speciality: it returns a *data* record (no internal index record),
 	*/
	@Override
	public AbstractRecord lookup(T key) {

		AbstractRecord leafRecord = this.uniqueBPlusTree.getLeafRecPrototype().clone();

		int foundPos = this.binarySearch(key);

		this.indexPage.read(foundPos, leafRecord);

		T keyValue = (T)leafRecord.getValue(UniqueBPlusTreeBase.KEY_POS);

		if(keyValue.equals(key)){
			return leafRecord;
		}

		return null;
	}

	/*
	   Leaf insertion has a speciality: it takes a *data* record (no internal index),
	   writes the data to the data heaptable and created a leaf-index-record on its own
	 */
	@Override
	public boolean insert(T key, AbstractRecord record){

		if(lookup(key) != null){
			return false; // record with that key exists
		}

		AbstractRecord leafRec = this.uniqueBPlusTree.getLeafRecPrototype().clone();

		RowIdentifier newRid = this.uniqueBPlusTree.getTable().insert(record); // insert data record to HeapTable
		// fill new leaf record with the given key and reference to the new slot just created
		leafRec.setValue(UniqueBPlusTreeBase.KEY_POS, key);
		leafRec.setValue(UniqueBPlusTreeBase.PAGE_POS, new SQLInteger(newRid.getPageNumber()));
		leafRec.setValue(UniqueBPlusTreeBase.SLOT_POS, new SQLInteger(newRid.getSlotNumber()));
		this.indexPage.insert(leafRec); // add leaf Record to index

		return true;
	}
	
	@Override
	public AbstractIndexElement<T> createInstance() {
		return new Leaf<T>(this.uniqueBPlusTree);
	}
}