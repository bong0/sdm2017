package de.tuda.sdm.dmdb.access.exercise;

import java.util.*;

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
				int bitmapLength = 4; // bitmap length = number of unique hash output values
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
					this.bitMaps.put(bitSetNameIt.next(), new BitSet(bitmapLength));
				}
				// fill bitmaps by iterating over table again
				tableIt = this.getTable().iterator(); // get new iterator from front of table
				int rowNumner = 0;
				while(tableIt.hasNext()){
					AbstractRecord rec = tableIt.next();
					T key = (T)rec.getValue(this.getKeyColumnNumber());
					//function to fill the bitmaps. (row number) modulo
					this.bitMaps.get(key).set(rowNumner % bitmapLength);
					rowNumner++; // we examine the next row now

					System.out.println(bitMaps.get(key));
				}
	}

	@Override
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

		BitSet mergedBitMap = new BitSet(this.getTable().getRecordCount());

		// for each key in range
		Iterator<T> keysInRangeIterator = keysInRange.iterator();
		while(keysInRangeIterator.hasNext()){
			// init new list of possible rows
			List<Integer> possibleRows = new LinkedList<>();

			T curKey = keysInRangeIterator.next();
			// for each row in bitmap
			BitSet curKeyBitset = this.bitMaps.get(curKey);

			// lookup all rows that have a 1 in the bitmap
			for (int i = curKeyBitset.nextSetBit(0); i >= 0; i = curKeyBitset.nextSetBit(i+1)) {
				// operate on index i here
				if (i == Integer.MAX_VALUE) {
					break; // or (i+1) would overflow
				}

				// determine possible rows for current index (reverse modulo)
				for(int possibleIndex=i; possibleIndex < this.getTable().getRecordCount(); possibleIndex+=4){
					// check if key matches searched one (else it's false positive)
					if(this.getTable().getRecordFromRowId(possibleIndex).getValue(this.keyColumnNumber).equals(curKey)){
						// set rowid in fullsize bitmap
						mergedBitMap.set(possibleIndex);
					}
				}

			}

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
