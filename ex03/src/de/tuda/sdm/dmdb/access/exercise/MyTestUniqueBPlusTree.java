package de.tuda.sdm.dmdb.access.exercise;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.AbstractUniqueIndex;
import de.tuda.sdm.dmdb.access.HeapTable;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;
import org.junit.Assert;

public class MyTestUniqueBPlusTree extends TestCase{
	
	/**
	 * Insert three records and reads them again using a SQLInteger index
	 */
	public void testIndexSimple(){
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractTable table = new HeapTable(record1.clone());
		
		AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);
		index.insert(record1);
		index.insert(record2);
		index.insert(record3);
		//index.print();

		AbstractRecord record1Cmp = index.lookup((SQLInteger) record1.getValue(0));
		Assert.assertEquals(record1, record1Cmp);
		
		AbstractRecord record2Cmp = index.lookup((SQLInteger) record2.getValue(0));
		Assert.assertEquals(record2, record2Cmp);
		
		AbstractRecord record3Cmp = index.lookup((SQLInteger) record3.getValue(0));
		Assert.assertEquals(record3, record3Cmp);
	}
	
	/**
	 * Insert three records and reads them again using a SQLVarchar index
	 */
	public void testIndexSimple2(){

		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractTable table = new HeapTable(record1.clone());
		
		AbstractUniqueIndex<SQLVarchar> index = new UniqueBPlusTree<SQLVarchar>(table, 1, 1);
		index.insert(record1);
		index.insert(record2);
		index.insert(record3);

		AbstractRecord record1Cmp = index.lookup((SQLVarchar) record1.getValue(1));
		Assert.assertEquals(record1, record1Cmp);
		
		AbstractRecord record2Cmp = index.lookup((SQLVarchar) record2.getValue(1));
		Assert.assertEquals(record2, record2Cmp);
		
		AbstractRecord record3Cmp = index.lookup((SQLVarchar) record3.getValue(1));
		Assert.assertEquals(record3, record3Cmp);
	}

	public void testIndexEnforce2ndlevel(){
		int ENTRIES_CREATE=100;

		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(-1));
		record1.setValue(1, new SQLVarchar("Hello.", 10));

		AbstractTable table = new HeapTable(record1.clone());

		// new index by first attrib (sqlint)
		AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table, 0, 1);

		for(int i=1; i<=ENTRIES_CREATE; i++){
			AbstractRecord recTmp = record1.clone();
			recTmp.setValue(0, new SQLInteger(i));
			recTmp.setValue(1, new SQLVarchar("Hello.", 10));
			index.insert(recTmp);
		}

		Assert.assertEquals(ENTRIES_CREATE, index.getTable().getRecordCount()); // verify all recs were written to datastore

		// test lookup of one of the records inserted late
		SQLInteger PROBE_NO=new SQLInteger(ENTRIES_CREATE-2);
		AbstractRecord record1Cmp = index.lookup(PROBE_NO);
		Assert.assertNotEquals(null, record1Cmp);
		record1.setValue(0, PROBE_NO);
		Assert.assertEquals(record1, record1Cmp);


		// lookup invalid key
		Assert.assertEquals(null, index.lookup(new SQLInteger(ENTRIES_CREATE+1)));
		Assert.assertEquals(null, index.lookup(new SQLInteger(-1)));


		//index.print();

	}

}
