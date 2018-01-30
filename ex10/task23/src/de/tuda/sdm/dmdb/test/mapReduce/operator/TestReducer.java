package de.tuda.sdm.dmdb.test.mapReduce.operator;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestReducer extends TestCase{
	

	public void testReducerIdentity() {

		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
		List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();

		AbstractRecord record = MapReduceOperator.keyValueRecordPrototype.clone();
		record.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("the", 100));
		record.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
		
		AbstractRecord expectedRecord = record.clone();
		expectedResult.add(expectedRecord);
		expectedResult.add(expectedRecord);

		HeapTable table1 = new HeapTable(record);
		table1.insert(record);
		table1.insert(record);

		TableScan ts = new TableScan(table1);
		Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> reducer = new Reducer<>();
		reducer.setChild(ts);
		reducer.open();
		AbstractRecord next;
		while ((next = reducer.next()) != null) {
			actualResult.add(next);
		}
		reducer.close();
		
		Assert.assertEquals(expectedResult.size(), actualResult.size());
		
		for (int i = 0; i < actualResult.size(); i++) {
			AbstractRecord value1 = expectedResult.get(i);
			AbstractRecord value2 = actualResult.get(i);
			Assert.assertEquals(value1, value2);
		}
	}
	
}
