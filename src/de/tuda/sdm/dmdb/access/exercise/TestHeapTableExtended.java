package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.RowIdentifier;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.exercise.RowPage;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;
import org.junit.Assert;

public class TestHeapTableExtended extends TestCase{
	/**
	 * Insert 2 records into a heap table and read them by their RID (RowIdentifier)
	 */
	public void testTable1(){
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));

		AbstractTable table = new HeapTable(record1.clone());
		RowIdentifier rid1 = table.insert(record1);

		/* Lookup invalid pagenos */
		boolean thrown=false;
		try {
			table.lookup(-1, 0);
		} catch (Exception e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);

		/* Lookup invalid slotnos */
		thrown=false;
		try {
			table.lookup(0, -1);
		} catch (Exception e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);
		thrown=false;
		try {
			table.lookup(0, 4);
		} catch (Exception e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);


		// fill up page until full and check the switching of pagenumbering
		int recsInserted = 0;
		System.out.println("numrecsexp "+RowPage.estimateRecords(table.getPrototype()));
		for(int i=0; i<RowPage.estimateRecords(table.getPrototype())-1; i++){
			table.insert(record1);
			recsInserted++;
		}

		// are all records saved?
		Assert.assertEquals(table.getRecordCount(), recsInserted+1);

		// Check if second page is still empty (it should be)
		thrown=false;
		try {
			table.lookup(1, 0);
		} catch (Exception e) {
			thrown = true;
		}
		Assert.assertEquals(true, thrown);
	}
}
