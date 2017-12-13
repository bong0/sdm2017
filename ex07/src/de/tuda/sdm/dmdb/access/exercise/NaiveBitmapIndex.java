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
			this.bitMaps.put(bitSetNameIt.next(), new BitSet(bitmapLength));
		}
		// fill bitmaps by iterating over table again
		tableIt = this.getTable().iterator(); // get new iterator from front of table
		int rowNumner = 0;
		while(tableIt.hasNext()){
			AbstractRecord rec = tableIt.next();
			T key = (T)rec.getValue(this.getKeyColumnNumber());

			this.bitMaps.get(key).set(rowNumner);
			rowNumner++; // we examine the next row now

			System.out.println(bitMaps.get(key));
		}

	}

	@Override
	//A record falls into the range if startKey <= recordKey <= endKey
	public List<AbstractRecord> rangeLookup(T startKey, T endKey) {

		// return null if invalid range specified
		if(startKey.compareTo(endKey) > 0){
			return null;
		}


		// determine all distinct values in the given range (create subset)
		List<T> keysInRange = new LinkedList<>();
		Iterator<T> allKeysIterator = getBitMaps().keySet().iterator();
		while(allKeysIterator.hasNext()){
			T curKey = allKeysIterator.next();
			// this is essentially:
			// startK<=curK becomes curK>startK
			// curKey<=endKey becomes endKey>startK  (<0 means arg is greater than, 0 means equal)
			System.out.println("comparing start to cur "+ startKey+ " "+curKey);
			if(startKey.compareTo(curKey) <= 0 && curKey.compareTo(endKey) <= 0){
				keysInRange.add(curKey);
			}
		}

		System.out.println("keysinrange "+keysInRange);

		// now get all bitmaps of the determined keysInRange
		// and merge the bitmaps found with OR
		BitSet mergedBitMap = new BitSet(this.bitmapSize);
		Iterator keysInRangeIterator = keysInRange.iterator();
		while(keysInRangeIterator.hasNext()){
			// fetch bitset for key and merge it with existing map by ORing
			mergedBitMap.or(this.bitMaps.get(keysInRangeIterator.next()));
		}

		List<AbstractRecord> listOfRecordsInRange = new ArrayList<>();

		// lookup all rows that have a 1 in the bitmap
		for (int i = mergedBitMap.nextSetBit(0); i >= 0; i = mergedBitMap.nextSetBit(i+1)) {
			// operate on index i here
			if (i == Integer.MAX_VALUE) {
				break; // or (i+1) would overflow
			}

			listOfRecordsInRange.add(this.getTable().getRecordFromRowId(i)); // fetch record by its row id in the bitmap index
		}

		return listOfRecordsInRange;
	}

}
