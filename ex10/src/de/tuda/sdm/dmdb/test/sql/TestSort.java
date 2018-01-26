package de.tuda.sdm.dmdb.test.sql;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.exercise.Sort;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestSort extends TestCase{

	public void testSort() {
		int numRecords = 10;
		AbstractRecord r = new Record(1);
		r.setValue(0, new SQLInteger(1));

		List<Integer> expectedResult = new ArrayList<Integer>();
		List<Integer> actualResult = new ArrayList<Integer>();

		HeapTable ht = new HeapTable(r);

		for (int i = 0; i < numRecords; i++) {
			Random rand = new Random(); 
			int value = rand.nextInt(numRecords);
			AbstractRecord newRecord = new Record(1);
			newRecord.setValue(0, new SQLInteger(value));

			ht.insert(newRecord);
			expectedResult.add(value);
		}
		expectedResult.sort(null);

		TableScan ts = new TableScan(ht);
		int compareColumn = 0; 
		Comparator<AbstractRecord> recordComparator = new Comparator<AbstractRecord>() {
			public int compare(AbstractRecord record1, AbstractRecord record2) {

				AbstractSQLValue value1 = record1.getValue(compareColumn);
				AbstractSQLValue value2 = record2.getValue(compareColumn);

				return value1.compareTo(value2);
			}
		};
		Sort sort = new Sort(ts, recordComparator);
		sort.open();
		AbstractRecord nextRecord;
		while((nextRecord = sort.next()) != null) {
			actualResult.add(((SQLInteger)nextRecord.getValue(compareColumn)).getValue());
		}
		sort.close();
		
		Assert.assertEquals(expectedResult.size(), actualResult.size());
		
		for (int i = 0; i < actualResult.size(); i++) {
			Integer value1 = expectedResult.get(i);
			Integer value2 = actualResult.get(i);
			Assert.assertEquals(value1, value2);
		}
	}
}
