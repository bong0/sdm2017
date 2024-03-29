package de.tuda.sdm.dmdb.test.access;

import java.util.List;

import org.junit.Assert;

import de.tuda.sdm.dmdb.access.AbstractBitmapIndex;
import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.exercise.ApproximateBitmapIndex;
import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestApproximateBitmapIndex extends TestCase{
	
	/**
	 * Insert four records and reads them again using a SQLInteger index
	 */
	public void testRangeLookupSimple(){
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractRecord record4 = new Record(2);
		record4.setValue(0, new SQLInteger(4));
		record4.setValue(1, new SQLVarchar("Hello114", 10));
		
		AbstractTable table = new HeapTable(record1.clone());

		table.insert(record1);
		table.insert(record2);
		table.insert(record3);
		table.insert(record3);
		
		AbstractBitmapIndex<SQLInteger> index = new ApproximateBitmapIndex<SQLInteger>(table, 0, 2);
		//index.print();

		List<AbstractRecord>  result = index.rangeLookup((SQLInteger) record1.getValue(0), (SQLInteger) record2.getValue(0));
		Assert.assertTrue(result.size() == 2);
		Assert.assertEquals(record1, result.get(0));
		Assert.assertEquals(record2, result.get(1));

		result = index.rangeLookup((SQLInteger) record3.getValue(0), (SQLInteger) record4.getValue(0));
		Assert.assertTrue(result.size() == 2);
		Assert.assertEquals(record3, result.get(0));
		Assert.assertEquals(record3, result.get(1));

		// check if null returned when range start is > start end;
		result = index.rangeLookup(new SQLInteger(5), new SQLInteger(1));
		Assert.assertEquals(null, result);

		// check point range
		result = index.rangeLookup(new SQLInteger(1), new SQLInteger(1));
		Assert.assertEquals(1, result.size());

		// check no entries in range but range valid
		AbstractRecord record5 = new Record(2);
		record5.setValue(0, new SQLInteger(10));
		record5.setValue(1, new SQLVarchar("Hello11328479", 10));
		table.insert(record5);

		AbstractBitmapIndex<SQLInteger> index2 = new ApproximateBitmapIndex<>(table, 0, 3);
		result = index2.rangeLookup(new SQLInteger(5), new SQLInteger(9));
		Assert.assertEquals(0, result.size());
		result = index2.rangeLookup(new SQLInteger(3), new SQLInteger(10));


		Assert.assertEquals(3, result.size());
		Assert.assertEquals(record3, result.get(0));
		Assert.assertEquals(record3, result.get(1));
		Assert.assertEquals(record5, result.get(2));

// create some many demo records
		for(int i=0; i< 100; i++) {
			AbstractRecord r = new Record(2);
			r.setValue(0, new SQLInteger(i));
			r.setValue(1, new SQLVarchar("Hello11328479", 10));
			table.insert(r);
		}
		AbstractBitmapIndex<SQLInteger> index3 = new ApproximateBitmapIndex<>(table, 0, 3);

		result = index3.rangeLookup(new SQLInteger(5), new SQLInteger(5));
		Assert.assertEquals(1, result.size());

		result = index3.rangeLookup(new SQLInteger(40), new SQLInteger(51));
		Assert.assertEquals(12, result.size());



	}
}
