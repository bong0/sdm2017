package de.tuda.sdm.dmdb.test.sql;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.test.TestCase;
import org.junit.Assert;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class TestShuffleUnbalanced extends TestCase{

	public void testShuffleSimple1() throws InterruptedException {

		AbstractRecord templateRecord = new Record(1);
		templateRecord.setValue(0, new SQLInteger(0));

		HeapTable htable1 = new HeapTable(templateRecord);
		HeapTable htable2 = new HeapTable(templateRecord);

		Queue<AbstractRecord> resultList = new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> resultList2 = new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> expectedResult = new LinkedList<AbstractRecord>();
		Queue<AbstractRecord> expectedResult2 = new LinkedList<AbstractRecord>();

		int numRecords = 50;
		for (int i = 0; i < numRecords; i++) {

			AbstractRecord recrod = new Record(1);
			recrod.setValue(0, new SQLInteger(i));

			if (i < 40) {
				htable1.insert(recrod);
			} else {
				htable2.insert(recrod);
			}

			int hashValue = i % 2;
			if (hashValue == 0) {
				expectedResult.add(recrod);
			} else {
				expectedResult2.add(recrod);
			}

		}

		int nodeId = 0;
		int port = 8000;
		Map<Integer, String> nodeMap = new HashMap<Integer, String>();
		nodeMap.put(nodeId, "localhost:" + port);
		nodeMap.put(nodeId + 1, "localhost:" + (port + 1));
		int partitionColumn = 0;


		Runnable task1 = () -> {
			TableScan tableScan0 = new TableScan(htable1);
			Shuffle shuffleOperator0 = new Shuffle(tableScan0, nodeId, nodeMap, port, partitionColumn);
			shuffleOperator0.open();
			AbstractRecord next;
			while ((next = shuffleOperator0.next()) != null) {
				System.out.println("node0: offering "+next);
				resultList.offer(next);
			}
			System.out.println("node0: offered null");

			shuffleOperator0.close();
		};

		Runnable task2 = () -> {
			TableScan tableScan1 = new TableScan(htable2);
			Shuffle shuffleOperator1 = new Shuffle(tableScan1, nodeId + 1, nodeMap, port + 1, partitionColumn);
			shuffleOperator1.open();
			AbstractRecord next;
			while ((next = shuffleOperator1.next()) != null) {
				System.out.println("node1: offering "+next);
				resultList2.offer(next);
			}
			System.out.println("node1: offered null");
			shuffleOperator1.close();
		};

		Thread peer1 = new Thread(task1);
		Thread peer2 = new Thread(task2);
		peer1.start(); peer2.start();
		try{
			peer1.join();
		} catch(InterruptedException e){
			e.printStackTrace();
		}

		try{
			peer2.join();
		} catch(InterruptedException e){
			e.printStackTrace();
		}
		System.out.println("Join finished");


		/*TableScan tableScan = new TableScan(htable1);
		tableScan.open();
		int a = 0;
		AbstractRecord gg = null;
		while((gg=tableScan.next())!= null){
			a++;
		}
		tableScan.close();

		System.out.println("Tablescan on ht1 returned itemcount "+a);*/



		System.out.println("Record list1: " + resultList);
		System.out.println("Record list2: " + resultList2);
		System.out.println("Expected Record list1: " + expectedResult);
		System.out.println("Expected Record list2: " + expectedResult2);
		Assert.assertEquals(expectedResult.size(), resultList.size());
		Assert.assertEquals(expectedResult2.size(), resultList2.size());
		for (AbstractRecord abstractRecord : resultList) {
			Assert.assertTrue(expectedResult.contains(abstractRecord));
		}
		for (AbstractRecord abstractRecord : resultList2) {
			Assert.assertTrue(expectedResult2.contains(abstractRecord));
		}

	}
}
