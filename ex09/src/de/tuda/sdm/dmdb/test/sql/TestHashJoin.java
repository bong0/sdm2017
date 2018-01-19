package de.tuda.sdm.dmdb.test.sql;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.exercise.HashJoin;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestHashJoin extends TestCase{

	public void testHashJoinNext() {

		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();

		AbstractRecord templateRecord = new Record(2);
		templateRecord.setValue(0, new SQLInteger(0));
		templateRecord.setValue(1, new SQLInteger(0));

		HeapTable htLeft = new HeapTable(templateRecord);
		HeapTable htRight = new HeapTable(templateRecord);

		int numRecords = 100;
		for (int i = 0; i < numRecords; i++) {
			AbstractRecord recordl = new Record(2);
			recordl.setValue(0, new SQLInteger(i));
			recordl.setValue(1, new SQLInteger(i));

			AbstractRecord recordr = new Record(2);
			recordr.setValue(0, new SQLInteger(i));
			recordr.setValue(1, new SQLInteger(i+5));

			htLeft.insert(recordl);
			htRight.insert(recordr);

			expectedResult.add(recordl.append(recordr));
		}

		TableScan tsLeft = new TableScan(htLeft);
		TableScan tsRight = new TableScan(htRight);

		HashJoin join = new HashJoin(tsLeft, tsRight, 1, 0);

		join.open();
		for (AbstractRecord abstractRecord : expectedResult) {
			AbstractRecord next = join.next();
			System.out.println(next);
			Assert.assertEquals(abstractRecord, next);
		}
	}

}
