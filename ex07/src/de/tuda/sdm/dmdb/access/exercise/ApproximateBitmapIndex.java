package de.tuda.sdm.dmdb.access.exercise;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import de.tuda.sdm.dmdb.access.AbstractBitmapIndex;
import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;

/**
 * Bitmap that uses the approximate bitmap index (compressed) approach
 * @author melhindi
 *
 * @param <T> Type of the key index by the index. While all abstractSQLValues subclasses can be used,
 * the implementation currently only support for SQLInteger type is guaranteed.
 */
public class ApproximateBitmapIndex<T extends AbstractSQLValue> extends AbstractBitmapIndex<T> {

	/*
	 * Constructor of ApproximateBitmapIndex
	 * This implementation uses modulo as hash function and only supports SQLInteger as data type
	 * @param table Table for which the bitmap index will be build
	 * @param keyColumnNumbner: index of the column within the passed table that should be indexed
	 * @param bitmapSize Size of for each bitmap, i.e., use (% bitmapSize) as hashfunction
	 */
	public ApproximateBitmapIndex(AbstractTable table, int keyColumnNumber, int bitmapSize) {
		super(table, keyColumnNumber);
		this.bitMaps = new HashMap<T, BitSet>();
		this.bitmapSize = bitmapSize;
		this.bulkLoadIndex();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void bulkLoadIndex() {
		// TODO implement this method	
		//determine length of bitmaps
				int bitmapLength = this.getTable().getRecordCount(); // bitmap length = record count in naive approach
				//determine number of unique values (count of bitmaps)
				HashSet<T> columnHashSet = new HashSet<T>(); // t is the specific type of the key (subclass of abstractsqlvalue)
				Iterator<AbstractRecord> tableIt = this.getTable().iterator();
				while(tableIt.hasNext()){
					AbstractRecord rec = tableIt.next();
					columnHashSet.add((T)rec.getValue(this.keyColumnNumber));
				}
				int bitmapCount = columnHashSet.size();

				// iterate over each unique value and create a bitmap for it
				Iterator<T> bitSetNameIt = columnHashSet.iterator();
				while(bitSetNameIt.hasNext()){
					//shortening the halving the size of each bitmap
					this.bitMaps.put(bitSetNameIt.next(), new BitSet(bitmapLength/2));
				}
				// fill bitmaps by iterating over table again
				tableIt = this.getTable().iterator(); // get new iterator from front of table
				int rowNumner = 0;
				while(tableIt.hasNext()){
					AbstractRecord rec = tableIt.next();
					T key = (T)rec.getValue(this.getKeyColumnNumber());
					//function to fill the bitmaps. (row number) modulo (half of bitmaps size)
					this.bitMaps.get(key).set(rowNumner % (bitmapLength/2));
					rowNumner++; // we examine the next row now

					System.out.println(bitMaps.get(key));
				}
	}

	@Override
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {
		// TODO implement this method
		
		return null;
	}

}
