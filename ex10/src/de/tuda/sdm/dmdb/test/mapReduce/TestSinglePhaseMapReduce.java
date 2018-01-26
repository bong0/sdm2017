package de.tuda.sdm.dmdb.test.mapReduce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.Configuration;
import de.tuda.sdm.dmdb.mapReduce.Job;
import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.sdm.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

public class TestSinglePhaseMapReduce extends BaseMapReduceTestCase{

	public static class TokenizerMapper
	extends Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger>{

		private final static int one = 1;
		private SQLVarchar word = new SQLVarchar(255);

		@Override
		public void map(SQLInteger key, SQLVarchar value, 
				Queue<AbstractRecord> outList) {

			StringTokenizer itr = new StringTokenizer(value.getValue());
			while (itr.hasMoreTokens()) {

				word.setValue(itr.nextToken());

				AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
				newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
				newRecord.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(one));

				outList.add(newRecord);
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> {

		private SQLInteger result = new SQLInteger();

		public void reduce(SQLVarchar key, Iterable<SQLInteger> values,
				Queue<AbstractRecord> outList) {
			int sum = 0;
			for (SQLInteger val : values) {
				sum += val.getValue();
			}

			result.setValue(sum);

			AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
			newRecord.setValue(MapReduceOperator.KEY_COLUMN, (SQLVarchar) key);
			newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());

			nextList.add(newRecord);
		}
	}

	public void testMapReduceSimple() {
		int stringLength = 100;
		String[] partitionStrings = {"the quick brown fox", "the fox ate the mouse", "how now brown cow"};
		List<HeapTable> partitions =  new ArrayList<HeapTable>();
		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
		this.initPartitionsAndExpectedResult(partitionStrings, stringLength, partitions, expectedResult);

		List<HeapTable> outputs = new ArrayList<HeapTable>();
		AbstractRecord outputPrototype =  new Record(2);
		outputPrototype.setValue(0, new SQLVarchar("key", stringLength));
		outputPrototype.setValue(1, new SQLInteger(0));

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputs(partitions);
		job.setOutputs(outputs);
		job.setOutputPrototype(outputPrototype);

		Assert.assertTrue(job.waitForCompletion());

		List<AbstractRecord> actualResult = new ArrayList<>();
		for (int i = 0; i < outputs.size(); i++) {
			HeapTable output = outputs.get(i);
			System.out.println("Result-Partition-" + i + ": " + output);	
			Iterator<AbstractRecord> it = output.iterator();
			while (it.hasNext()) {
				actualResult.add(it.next());
			}
		}

		System.out.println("Done");
		expectedResult.sort(recordComparator);
		actualResult.sort(recordComparator);
		System.out.println("Expected sorted: " + expectedResult);
		System.out.println("Actual sorted  : " + actualResult);
		Assert.assertTrue(outputs.size() == conf.getNumReducers());
		Assert.assertEquals(expectedResult.size(), actualResult.size());
		for (AbstractRecord abstractRecord : expectedResult) {
			Assert.assertTrue(actualResult.contains(abstractRecord));
		}
	}

}
