package de.tuda.sdm.dmdb.access.exercise;

import java.util.*;

import de.tuda.sdm.dmdb.access.AbstractBitmapIndex;
import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Bitmap index that uses the vanilla/naive bitmap approach (one bitmap for each distinct value)
 * @author melhindi
 *
 ** @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be used,
 * the implementation currently only support for SQLInteger type is guaranteed.
 */
public class NaiveBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T>{

	/*
	 * Constructor of NaiveBitmapIndex
	 * @param table Table for which the bitmap index will be build
	 * @param keyColumnNumbner: index of the column within the passed table that should be indexed
	 */
	public NaiveBitmapIndex(AbstractTable table, int keyColumnNumber) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
		this.bitmapSize = this.getTable().getRecordCount();
		this.bulkLoadIndex();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		//determine length of bitmaps
		int bitmapLength = this.getTable().getRecordCount(); // bitmap length = record count in naive approach
		//determine number of unique values (count of bitmaps)
		HashSet<AbstractSQLValue> columnHashSet = new HashSet<AbstractSQLValue>();
		Iterator<AbstractRecord> tableIt = this.getTable().iterator();
		while(tableIt.hasNext()){
			AbstractRecord rec = tableIt.next();
			columnHashSet.add(rec.getValue(this.keyColumnNumber));
		}
		int bitmapCount = columnHashSet.size();

		// create bitmapCount bitmaps and fill them by iterating over table again, setting each entry that has given unique value
		
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		// TODO implement this method

		return null;
	}

}
